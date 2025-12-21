#include "synchronizationOps.hpp"
#include "../common/config.hpp"
#include "../common/decoder.hpp"
#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "../common/responseDecoder.hpp"
#include "config.hpp"
#include "connect.hpp"
#include "lcp.hpp"
#include "registration.hpp"
#include <cstring>
#include <deque>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

static std::unordered_map<int, time_t> newConnTimeoutMap;
static int epollFd;
static int newConnTimerFd;
static int pingResponseSocketPairFds[2];
static string lcpID;

static void onConnectionClose(int);
static void asyncConnect(int);
static void registerConnection(int);
static void startConnTimeout();

void pingResponseHandler() {
  // Decode response > fetch numberOfConnections to initiate
  // Asynchronously initiate new connections
  //
  // Remove from epoll monitoring
  // Close current socket pair

  logger("CacheSynchronization thread : In pingResponseHandler");
  // This is synchronous read
  // GlobalCacheOps thread will trigger pingResponseEventFd after writing data
  // to pingResponseSocketPairFds[1], thus all readable data should be present
  // at this point

  char buf[1024];
  int readBytes =
      read(pingResponseSocketPairFds[0], &buf, configLCP.MAX_READ_BYTES);
  logger("cacheSynchronization thread : pingResponseFd readBytes : ",
         readBytes);

  if (readBytes <= 0) {
    // Connection closed by peer
    logger("No bytes read, closed by GCP : pingResponseSocketPairFds[0] : ",
           pingResponseSocketPairFds[0]);
  }

  logger("CacheSynchronization thread : Removing from epoll monitoring "
         "pingResponseSocketPairFds[0] : ",
         pingResponseSocketPairFds[0]);
  if (epoll_ctl(epollFd, EPOLL_CTL_DEL, pingResponseSocketPairFds[0],
                nullptr) == -1) {
    logger("CacheSynchronization thread : Error while removing from epoll "
           "monitoring, pingResponseSocketPairFds[0] : ",
           pingResponseSocketPairFds[0]);
    perror("CacheSynchronization thread : Epoll_DEL");
    // Don't throw error here, lets this fd leak
  }

  close(pingResponseSocketPairFds[0]);
  // This may cause double close in rare edge scenario where it has already been
  // closed by GlobalCacheOps thread on error & same fd is assigned to a new
  // socket connection at the same time
  close(pingResponseSocketPairFds[1]);

  // Decode & initiate new connections
  string pingResponse;
  pingResponse.append(buf, readBytes);
  logger("CacheSynchronization thread : Decoding ping respone :  ",
         pingResponse);
  DecodedResponse decodedPingResponse = responseDecoder(pingResponse);

  int numberOfConnections = stoi(decodedPingResponse.data);
  logger(
      "CacheSynchronization thread : ping response with numberOfConnections : ",
      numberOfConnections);
  if (numberOfConnections > 0) {
    logger("CacheSynchronization thread : Asynchronously connecting "
           "numberOfConnections : ",
           numberOfConnections);
    asyncConnect(numberOfConnections);
  }
}

void epollIO(int epollFd, struct epoll_event *events,
             std::deque<ReadSocketMessage> &readSocketQueue, int timeout,
             int pingTimerFd, bool &poolHealthCheckEvent,
             bool &newConnHealthCheckEvent) {

  int readyFds =
      epoll_wait(epollFd, events, configLCP.MAX_SYNC_CONNECTIONS, timeout);

  if (readyFds == -1) {
    perror("cacheSynchronization thread : epoll_wait error");
    exit(EXIT_FAILURE);
  }

  for (int n = 0; n < readyFds; ++n) {
    int fd = events[n].data.fd;
    uint32_t ev = events[n].events;

    if (fd == pingTimerFd || fd == newConnTimerFd ||
        fd == pingResponseSocketPairFds[0]) {
      logger("CacheSynchronization thread : Reading from eventfd");

      while (true) {
        uint64_t counter;
        int count = read(fd, &counter, sizeof(counter));
        if (count == -1) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // No more data to read
            logger("cacheSynchronization thread : Drained fd");
            break;
          } else {
            perror("cacheSynchronization thread : Error while reading from fd");
            break;
          }
        }
        logger("cacheSynchronization thread : Read fd counter : ", counter);
      }

      if (fd == pingTimerFd) {
        logger("CacheSynchronization thread : Enabling poolHealthCheckEvent");
        poolHealthCheckEvent = true;
      }

      if (fd == newConnTimerFd) {
        logger(
            "CacheSynchronization thread : Enabling newConnHealthCheckEvent");
        newConnHealthCheckEvent = true;
      }

      if (fd == pingResponseSocketPairFds[0]) {
        logger("CacheSynchronization thread : pingResponseSocketPairFds[0] : ",
               pingResponseSocketPairFds[0]);
        pingResponseHandler();
      }

    } else {

      if (ev & EPOLLOUT) {
        registerConnection(fd);
      } else if (ev & (EPOLLHUP | EPOLLERR)) {
        onConnectionClose(fd);
      } else if (ev & EPOLLIN) {
        // Add to readSocketQueue
        logger("cacheSynchronization thread : Adding to readSocketQueue : ",
               events[n].data.fd);
        ReadSocketMessage sock;
        sock.fd = fd;
        sock.data = "";
        sock.readBytes = 0;
        readSocketQueue.push_back(sock);
      }
    }
  }
}

void readFromSocketQueue(
    std::deque<ReadSocketMessage> &readSocketQueue,
    moodycamel::ConcurrentQueue<Operation> &SynchronizationQueue,
    std::deque<WriteSocketMessage> writeSocketQueue) {
  // Read few bytes
  // If socket still has data, re-queue

  long pos = 0;
  long currentSize = readSocketQueue.size();

  while (pos < currentSize) {
    char buf[1024];
    ReadSocketMessage msg = readSocketQueue.front();
    readSocketQueue.pop_front();
    pos++;

    int readBytes = read(msg.fd, buf, configLCP.MAX_READ_BYTES);
    if (readBytes == 0) {
      // Connection closed by peer
      onConnectionClose(msg.fd);
    } else if (readBytes < 0) {
      logger("cacheSynchronization thread : readBytes < 0, for fd : ", msg.fd);
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // No data now, skip
      } else {
        // Other error, clean up
        onConnectionClose(msg.fd);
      }
    } else if (readBytes > 0) {
      msg.readBytes += readBytes;
      msg.data.append(buf, readBytes);
    }

    if (readBytes == configLCP.MAX_READ_BYTES) {
      // more data to read, Re-queue
      readSocketQueue.push_back(msg);
    } else if (readBytes > 0) {
      // Parse the message here & push to SynchronizationQueue
      DecodedMessage parsed = decoder(msg.data);
      if (parsed.error.partial) {
        // If message is parsed partially, re-queue in readSocketQueue
        logger("cacheSynchronization thread : Partially parsed, re-queuing");
        readSocketQueue.push_back(msg);
      } else if (parsed.error.invalid) {
        logger("cacheSynchronization thread : Invalid message : ", msg.data);
        onConnectionClose(msg.fd);
      } else if (parsed.operation == "SYNC_PING") {
        // Respond to GCP ping instead of pushing to SynchronizationQueue
        string res = "1";
        WriteSocketMessage response;
        response.fd = msg.fd;
        response.response = encoder(&res, "integer");
        writeSocketQueue.push_back(response);
      } else if (parsed.operation == "SYNC_CONN_ESTABLISHED") {
        // Don't push response from connection establish to SynchronizationQueue
        // Erase from connection timeout map
        logger("CacheSynchronization thread : Successfully establised "
               "connection with GCP : fd : ",
               msg.fd);

        logger("CacheSynchronization thread : Removing from newConnTimeoutMap "
               ": fd : ",
               msg.fd);
        newConnTimeoutMap.erase(msg.fd);
      } else {
        logger("cacheSynchronization thread : Successfully parsed, adding to "
               "SynchronizationQueue");
        drainSocketSync(msg.fd);
        Operation op;
        op.fd = msg.fd;
        op.msg = parsed;
        SynchronizationQueue.enqueue(op);
      }
    }
  }
}

void triggerEventfd(int synchronizationEventFd) {
  logger("cacheSynchronization thread : triggerEventfd for "
         "synchronizationEventFd");
  uint64_t counter = 1;
  write(synchronizationEventFd, &counter, sizeof(counter));
  logger("cacheSynchronization thread : synchronizationEventFd counter : ",
         counter);
}

void registerConnection(int conn) {
  logger(
      "cacheSynchronization thread : registering connection with GCP : conn : ",
      conn);
  int err;
  socklen_t socklen;

  // Get socket options
  logger("cacheSynchronization thread : fetching socket options : conn : ",
         conn);
  getsockopt(conn, SOL_SOCKET, SO_ERROR, &err, &socklen);

  if (err != 0) {
    logger(
        "cacheSynchronization thread : Error while connecting to GCP : conn : ",
        conn);
    onConnectionClose(conn);
    return;
  }

  // Remove from epoll monitoring
  logger(
      "cacheSynchronization thread : removing from epoll monitoring : conn : ",
      conn);
  epoll_ctl(epollFd, EPOLL_CTL_DEL, conn, NULL);

  // Re-register for epoll monitor with EPOLLIN
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = conn;

  logger("cacheSynchronization thread : adding connection : ", conn,
         " for monitoring by epoll");

  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, conn, &ev) == -1) {
    perror("cacheSynchronization thread : epoll_ctl : conn");
    onConnectionClose(conn);
    return;
  }

  vector<QueryArrayElement> regMessage =
      connRegisterationMessage(configCommon::RECEIVER_CONNECTION_TYPE, lcpID);
  string regQuery = encoder(regMessage);

  // Write to GCP with registration message
  int writtenBytes = write(conn, regQuery.c_str(), regQuery.length());
  if (writtenBytes < regQuery.length()) {
    logger("SENDER_CONNECTION_TYPE thread : Error while writing to GCP for "
           "connection "
           "registeration : conn : ",
           conn, " writtenBytes : ", writtenBytes);
    onConnectionClose(conn);
  }
}

int createPingTimer() {
  logger("cacheSynchronization thread : In createPingTimer");
  int timerFd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
  logger("cacheSynchronization thread : timerFd : ", timerFd);

  if (timerFd == -1) {
    perror("cacheSynchronization thread : timerFd");
    exit(EXIT_FAILURE);
  }

  struct itimerspec timerValue;
  timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
    logger("cacheSynchronization thread : Error while getting now");
    perror("cacheSynchronization thread : now");
    exit(EXIT_FAILURE);
  }

  memset(&timerValue, 0, sizeof(timerValue));
  timerValue.it_value.tv_sec =
      now.tv_sec + configLCP.CONNECTION_POOL_HEALTH_CHECK_INTERVAL;
  timerValue.it_interval.tv_sec =
      configLCP.CONNECTION_POOL_HEALTH_CHECK_INTERVAL;

  // Configure Edge triggered
  logger("cacheSynchronization thread : Add timerFd for monitoring");
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = timerFd;

  // Add timerfd descriptor for monitoring
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, timerFd, &ev) == -1) {
    perror("cacheSynchronization thread : epoll_ctl timerFd");
    exit(EXIT_FAILURE);
  }

  logger("cacheSynchronization thread : arm timer");
  if (timerfd_settime(timerFd, TFD_TIMER_ABSTIME, &timerValue, NULL) == -1) {
    perror("cacheSynchronization thread : timerfd_settime");
    exit(EXIT_FAILURE);
  }

  return timerFd;
}

void asyncConnect(int numberOfConnections) {
  logger(
      "CacheSynchronization thread : In asyncConnect : numberOfConnections : ",
      numberOfConnections);
  for (int i = 0; i < numberOfConnections; i++) {
    logger(
        "cacheSynchronization thread : Establishing connection asynchronously");
    int connSockFd = establishConnection(configLCP.GCP_SERVER_IP.c_str(),
                                         configLCP.GCP_SERVER_PORT, true);

    if (connSockFd == -1) {
      logger(
          "cacheSynchronization thread : Error in establishConnection async, "
          "connSockFd : ",
          connSockFd);
      // let this be handled in next iteration
      continue;
    }

    struct epoll_event ev;
    ev.events = EPOLLOUT | EPOLLET;
    ev.data.fd = connSockFd;

    logger("cacheSynchronization thread : adding connection : ", connSockFd,
           " for monitoring by epoll");
    if (epoll_ctl(epollFd, EPOLL_CTL_ADD, connSockFd, &ev) == -1) {
      perror("cacheSynchronization thread : epoll_ctl : connSockFd");
      // let this be handled in next iteration
      close(connSockFd);
      continue;
    }

    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();
    newConnTimeoutMap[connSockFd] = now;
  }
  logger("cacheSynchronization thread : Starting new conn timeout");
  startConnTimeout();
}

void onConnectionClose(int fd) {

  logger("CacheSynchronization thread : In onConnectionClose : fd : ", fd);

  logger("CacheSynchronization thread : Removing from epoll monitoring : fd : ",
         fd);
  if (epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
    logger("CacheSynchronization thread : Error while removing from epoll "
           "monitoring, fd : ",
           fd);
    perror("CacheSynchronization thread : Epoll_DEL");
  }

  close(fd);
  asyncConnect(1);
}

void newConnHealthCheck() {
  // Close all irresponsive connections present in newConnHealthCheck
  logger("cacheSynchronization thread : In newConnAndPingHealthCheck");
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  logger("cacheSynchronization thread : now : ", now,
         " newConnTimeoutMap size : ", newConnTimeoutMap.size());
  for (auto it = newConnTimeoutMap.begin(); it != newConnTimeoutMap.end();) {
    if (now - it->second >= configLCP.PING_CHECK_INTERVAL * 1000) {
      int fd = it->first;
      it = newConnTimeoutMap.erase(it);
      onConnectionClose(fd);
    } else {
      ++it;
    }
  }
}

void createNewConnTimer() {
  logger("cacheSynchronization thread : In createNewConnTimer");

  newConnTimerFd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
  logger("cacheSynchronization thread : newConnTimerFd : ", newConnTimerFd);

  if (newConnTimerFd == -1) {
    perror("cacheSynchronization thread : newConnTimerFd");
    exit(EXIT_FAILURE);
  }

  // Add to epoll monitoring
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = newConnTimerFd;

  logger("cacheSynchronization thread : Adding newConnTimerFd : ",
         newConnTimerFd, " for monitoring by epoll");
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, newConnTimerFd, &ev) == -1) {
    perror("cacheSynchronization thread : epoll_ctl: newConnTimerFd");
    exit(EXIT_FAILURE);
  }
}

void startConnTimeout() {
  logger("cacheSynchronization thread : In startConnTimeout");
  logger("cacheSynchronization thread : arming timer");

  timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
    logger("cacheSynchronization thread : Error while getting now");
    perror("cacheSynchronization thread : now");
    // Don't throw error here
    // Let another iteration handle this check
    return;
  }

  struct itimerspec newConnCheckTimerValue;
  memset(&newConnCheckTimerValue, 0, sizeof(newConnCheckTimerValue));

  newConnCheckTimerValue.it_value.tv_sec =
      now.tv_sec + configLCP.PING_CHECK_INTERVAL;
  newConnCheckTimerValue.it_interval.tv_sec =
      0; // To be ran only one, after intitating connection

  logger("cacheSynchronization thread : newConnTimerFd : ", newConnTimerFd);
  if (timerfd_settime(newConnTimerFd, TFD_TIMER_ABSTIME,
                      &newConnCheckTimerValue, NULL) == -1) {
    perror("cacheSynchronization thread : timerfd_settime : pingTimer");
    // Don't throw error here
    // Let further iterations handle this
    return;
  }
}

void startPoolHealthCheck(moodycamel::ConcurrentQueue<GlobalCacheOpMessage>
                              &cacheSynchronizationQueue,
                          int globalCacheThreadEventFd) {
  logger("cacheSynchronization thread : In startPoolHealthCheck");

  // Create socketpair for communication between GlobalCacheOps &
  //  synchronizationOps thread for PING response
  // Create new for every ping, as this can be closed in GlobalCacheOps thread
  // Add for epoll monitoring

  int pairResponse =
      socketpair(AF_UNIX, SOCK_STREAM, 0, pingResponseSocketPairFds);
  if (pairResponse == -1) {
    logger("CacheSynchronization thread : Error while creating socketpair for "
           "pingResponseSocketPairFds");
    perror("CacheSynchronization thread : Error while creating socketpair");
    // let another iteration handle this
    return;
  }

  // Add for monitoring
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = pingResponseSocketPairFds[0];

  logger(
      "cacheSynchronization thread : Adding  pingResponseSocketPairFds[0] : ",
      pingResponseSocketPairFds[0], " for monitoring by epoll");
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, pingResponseSocketPairFds[0], &ev) ==
      -1) {
    perror("cacheSynchronization thread : epoll_ctl : "
           "pingResponseSocketPairFds[0]");
    close(pingResponseSocketPairFds[0]);
    close(pingResponseSocketPairFds[1]);
    // let another iteration handle PING
    return;
  }

  // Send message to cacheSynchronizationQueue > trigger
  // globalCacheThreadEventFd
  vector<QueryArrayElement> pingArray;
  QueryArrayElement pingMessage;
  pingMessage.type = "string";
  pingMessage.value = "SYNC_PING";
  pingArray.push_back(pingMessage);
  string syncPing = encoder(pingArray);

  GlobalCacheOpMessage msg;
  msg.fd = pingResponseSocketPairFds[1];
  msg.op = syncPing;

  logger("CacheSynchronization thread : Enquing syncping message to "
         "cacheSynchronizationQueue : ",
         syncPing);
  cacheSynchronizationQueue.enqueue(msg);

  logger("cacheSynchronization thread : triggering globalCacheThreadEventFd : ",
         globalCacheThreadEventFd);
  uint64_t counter = 1;
  write(globalCacheThreadEventFd, &counter, sizeof(counter));
  logger("cacheSynchronization thread : globalCacheThreadEventFd counter : ",
         counter);
}

static void
writeToSocketQueue(std::deque<WriteSocketMessage> &writeSocketQueue) {
  // Write few bytes & keep writing till whole content is written

  long pos = 0;
  long currentSize = writeSocketQueue.size();

  logger("cacheSynchronization : In writeSocketQueue");
  while (pos < currentSize) {
    WriteSocketMessage msg = writeSocketQueue.front();
    logger("cacheSynchronization : writeSocketQueue : fd : ", msg.fd,
           " msg : ", msg.response);
    int msgLength = msg.response.length();
    int writtenBytes = write(msg.fd, msg.response.c_str(), msgLength);

    if (writtenBytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Try again later, re-queue the whole message
        logger("cacheSynchronization : Got EAGAIN while writing to fd : ",
               msg.fd, " requeuing");
        writeSocketQueue.push_back(msg);
      } else {
        logger("cacheSynchronization : Fatal write error, closing connection "
               "for fd: ",
               msg.fd);
        onConnectionClose(msg.fd);
      }
    } else if (writtenBytes == 0) {
      logger("cacheSynchronization : Write returned zero (connection closed?), "
             "fd: ",
             msg.fd);
      onConnectionClose(msg.fd);
    } else if (writtenBytes < msgLength) {
      logger("cacheSynchronization : Partial write, requeuing for fd : ",
             msg.fd);
      msg.response = msg.response.substr(writtenBytes);
      writeSocketQueue.push_back(msg);
    }

    writeSocketQueue.pop_front();
    pos++;
  }
}

void cacheSynchronization(
    moodycamel::ConcurrentQueue<Operation> &SynchronizationQueue,
    int synchronizationEventFd, string lcpId,
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage>
        &cacheSynchronizationQueue,
    int globalCacheThreadEventFd) {

  logger(
      "CacheSynchronization thread : Starting cacheSynchronization : lcpId : ",
      lcpId);
  lcpID = lcpId;

  // Initialize epoll
  int maxEpollEvent = configLCP.MAX_SYNC_CONNECTIONS +
                      3; // sync conns + pingTimerFd + newConnTimerFd +
                         // pingResponseSocketPairFds[0]

  struct epoll_event ev, events[maxEpollEvent];

  epollFd = epoll_create1(0);
  if (epollFd == -1) {
    perror("epoll create error");
    exit(EXIT_FAILURE);
  }

  // Asynchronously establish connections with GCP
  asyncConnect(configLCP.MAX_SYNC_CONNECTIONS);

  int pingTimerFd = createPingTimer();
  logger("CacheSynchronization thread : pingTimerFd : ", pingTimerFd);
  createNewConnTimer();
  logger("CacheSynchronization thread : newConnTimerFd : ", newConnTimerFd);

  int timeout = -1;
  std::deque<ReadSocketMessage> readSocketQueue;
  bool newConnHealthCheckEvent;
  bool poolHealthCheckEvent;
  std::deque<WriteSocketMessage> writeSocketQueue;

  while (1) {
    epollIO(epollFd, events, readSocketQueue, timeout, pingTimerFd,
            poolHealthCheckEvent, newConnHealthCheckEvent);
    readFromSocketQueue(readSocketQueue, SynchronizationQueue,
                        writeSocketQueue);
    triggerEventfd(synchronizationEventFd);
    writeToSocketQueue(writeSocketQueue);

    if (poolHealthCheckEvent) {
      startPoolHealthCheck(cacheSynchronizationQueue, globalCacheThreadEventFd);
      poolHealthCheckEvent = false;
    }

    if (newConnHealthCheckEvent) {
      newConnHealthCheck();
      newConnHealthCheckEvent = false;
    }

    // Set timeout to 0, if there is pending message in queue
    // else to -1 ( Infinity )
    if (readSocketQueue.size() || writeSocketQueue.size()) {
      timeout = 0;
    } else {
      timeout = -1;
    }
  }
}
