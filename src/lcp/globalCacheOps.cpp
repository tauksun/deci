#include "globalCacheOps.hpp"
#include "../common/common.hpp"
#include "../common/config.hpp"
#include "../common/encoder.hpp"
#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "../common/responseDecoder.hpp"
#include "config.hpp"
#include "connect.hpp"
#include "registration.hpp"
#include <asm-generic/socket.h>
#include <cstring>
#include <ctime>
#include <deque>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <time.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

std::deque<int> connectionPool;
std::unordered_map<int, int> socketReqResMap;
std::unordered_map<int, time_t> timeMap;
std::unordered_set<int> activeConnections;
std::unordered_map<int, time_t> newConnAndPingTimeoutMap;

int epollFd;
int pingTimerFdValue;
string lcpID;

void onConnectionClose(int);
void registerConnection(int);

void epollIO(int epollFd, struct epoll_event *events,
             std::deque<ReadSocketMessage> &readSocketQueue, int timeout,
             int globalCacheThreadEventFd, int timerFd,
             bool &poolHealthCheckEvent, int pingTimerFd,
             bool &newConnAndPingHealthCheckEvent) {
  logger("globalCacheOps thread : In epollIO");
  int readyFds =
      epoll_wait(epollFd, events, configLCP.MAX_GCP_CONNECTIONS, timeout);

  if (readyFds == -1) {
    perror("globalCacheOps thread : epoll_wait error");
    exit(EXIT_FAILURE);
  }

  logger("globalCacheOps thread : Looping on readyFds : ", readyFds);
  for (int n = 0; n < readyFds; ++n) {
    int fd = events[n].data.fd;
    uint32_t ev = events[n].events;

    if (fd == globalCacheThreadEventFd || fd == timerFd || fd == pingTimerFd) {

      if (fd == timerFd) {
        logger("globalCacheOps thread : timerFd is ready");
        poolHealthCheckEvent = true;
      }

      if (fd == pingTimerFd) {
        logger("globalCacheOps thread : pingTimerFd is ready");
        newConnAndPingHealthCheckEvent = true;
      }

      logger("globalCacheOps thread : Reading from fd");
      while (true) {
        uint64_t counter;
        int count = read(fd, &counter, sizeof(counter));
        if (count == -1) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // No more data to read
            logger("globalCacheOps thread : Drained fd");
            break;
          } else {
            perror("globalCacheOps thread : Error while reading from fd");
            break;
          }
        }
        logger("globalCacheOps thread : Read fd counter : ", counter);
      }
    } else {
      if (ev & EPOLLOUT) {
        registerConnection(fd);
      } else if (ev & (EPOLLHUP | EPOLLERR)) {
        onConnectionClose(fd);
      } else if (ev & EPOLLIN) {
        // Add to readSocketQueue
        logger("globalCacheOps thread : Adding to readSocketQueue : ", fd);
        ReadSocketMessage sock;
        sock.fd = fd;
        sock.data = "";
        sock.readBytes = 0;
        readSocketQueue.push_back(sock);
      }
    }
  }
}

void readFromSocketQueue(std::deque<ReadSocketMessage> &readSocketQueue,
                         std::deque<WriteSocketMessage> &writeSocketQueue) {
  // Read few bytes
  // If socket still has data, re-queue

  logger("globalCacheOps thread : In readFromSocketQueue");
  long pos = 0;
  long currentSize = readSocketQueue.size();
  logger("globalCacheOps thread : readFromSocketQueue size : ", currentSize);

  while (pos < currentSize) {
    char buf[1024];
    ReadSocketMessage msg = readSocketQueue.front();
    readSocketQueue.pop_front();
    pos++;

    int readBytes = read(msg.fd, buf, configLCP.MAX_READ_BYTES);
    logger("globalCacheOps thread : Read ", readBytes,
           " bytes from fd : ", msg.fd);
    if (readBytes == 0) {
      // Connection closed by peer
      logger("No bytes read, closing connection for fd : ", msg.fd);
      onConnectionClose(msg.fd);
      continue;
    } else if (readBytes < 0) {
      logger("globalCacheOps thread : readBytes < 0, for fd : ", msg.fd);
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // No data now, skip
        logger("globalCacheOps thread : Received EAGAIN for fd : ", msg.fd);
      } else {
        // Other error, clean up
        logger("globalCacheOps thread : Error occured while reading for fd : ",
               msg.fd);
        onConnectionClose(msg.fd);
      }
      continue;
    } else if (readBytes > 0) {
      msg.readBytes += readBytes;
      msg.data.append(buf, readBytes);
    }

    if (readBytes == configLCP.MAX_READ_BYTES) {
      // more data to read, Re-queue
      readSocketQueue.push_back(msg);
    } else if (readBytes > 0) {
      // Decode the response
      logger("globalCacheOps thread : Decoding response on fd : ", msg.fd,
             " response : ", msg.data);
      DecodedResponse decodedResponse = responseDecoder(msg.data);
      if (decodedResponse.error.partial) {
        // If response is decoded partially, re-queue in readSocketQueue
        logger("globalCacheOps thread : Partially decoded, re-queuing");
        readSocketQueue.push_back(msg);
      } else if (decodedResponse.error.invalid) {
        logger("globalCacheOps thread : Invalid response, closing connection "
               "for fd : ",
               msg.fd, " response : ", msg.data);
        onConnectionClose(msg.fd);
      } else {
        logger(
            "globalCacheOps thread : Successfully decoded response for fd : ",
            msg.fd);
        drainSocketSync(msg.fd);

        // Write to Socket Queue for Application logic
        // Else > Add back to connection pool & Remove from activeConnections

        auto val = socketReqResMap.find(msg.fd);
        if (val != socketReqResMap.end()) {
          logger("globalCacheOps thread : adding to writeSocketQueue : ",
                 val->second);
          WriteSocketMessage response;
          response.fd = val->second; // Fetched from the hashmap
          response.response = msg.data;
          writeSocketQueue.push_back(response);
          logger("globalCacheOps thread : Erasing entry from socketReqResMap "
                 "for : ",
                 msg.fd);
          socketReqResMap.erase(msg.fd);
        }

        logger("globalCacheOps thread : adding ", msg.fd,
               " back to connection pool");
        connectionPool.push_back(msg.fd);

        logger(
            "globalCacheOps thread : Removing from activeConnections : conn : ",
            msg.fd);
        activeConnections.erase(msg.fd);

        // This is irrelevant for normal queries
        logger("globalCacheOps thread : Removing from newConnAndPingTimeoutMap "
               ": conn : ",
               msg.fd);
        newConnAndPingTimeoutMap.erase(msg.fd);
        logger("globalCacheOps thread : connectionPool size : ",
               connectionPool.size());

        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
        logger("globalCacheOps thread : Updating timeMap for fd : ", msg.fd,
               " now : ", now);
        timeMap[msg.fd] = now;
      }
    }
  }
}

void writeToApplicationSocket(
    std::deque<WriteSocketMessage> &writeSocketQueue) {
  // Write few bytes & keep writing till whole content is written

  long pos = 0;
  long currentSize = writeSocketQueue.size();

  logger("globalCacheOps thread : In writeToApplicationSocket");

  while (pos < currentSize) {
    WriteSocketMessage response = writeSocketQueue.front();
    writeSocketQueue.pop_front();
    pos++;

    logger("globalCacheOps thread : Writing to fd : ", response.fd,
           " response : ", response.response);

    int responseLength = response.response.length();
    int writtenBytes =
        write(response.fd, response.response.c_str(), responseLength);

    if (writtenBytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Try again later, re-queue the whole message
        logger("globalCacheOps thread : Got EAGAIN while writing to fd : ",
               response.fd, " requeuing");
        writeSocketQueue.push_back(response);
      } else {
        logger("globalCacheOps thread : Fatal write error, closing connection "
               "for fd: ",
               response.fd);
        close(response.fd);
      }
    } else if (writtenBytes == 0) {
      logger("globalCacheOps thread : Write returned zero (connection "
             "closed?), fd: ",
             response.fd);
      close(response.fd);
    } else if (writtenBytes < responseLength) {
      logger("globalCacheOps thread : Partial write, requeuing for fd : ",
             response.fd);
      response.response = response.response.substr(writtenBytes);
      writeSocketQueue.push_back(response);
    }
  }
}

void readFromConcurrentQueue(
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue) {

  logger("globalCacheOps thread : In readFromConcurrentQueue");

  // check connectionPool > if available : send query to GCP
  for (int i = 0;; i++) {
    GlobalCacheOpMessage msg;
    bool found = GlobalCacheOpsQueue.try_dequeue(msg);
    if (!found) {
      logger("globalCacheOps thread : GlobalCacheOpsQueue is empty");
      break;
    }

    if (!msg.partial) {
      logger("globalCacheOps thread : msg is new, checking for connectionPool "
             "availability");
      int isConnectionAvailable = connectionPool.size();
      if (!isConnectionAvailable) {
        logger("globalCacheOps thread : connection pool is empty");
        logger("globalCacheOps thread : Re-queuing the extracted message");
        GlobalCacheOpsQueue.enqueue(msg);
        break;
      }

      logger("globalCacheOps thread : Connection pool has : ",
             isConnectionAvailable, " connections");
    }

    // Write to a socket connection from connectionPool
    int connSock;
    if (msg.partial) {
      // Use already used connection to send more data
      logger("globalCacheOps thread : Using existing connection for partial "
             "msg, connSock : ",
             connSock);
      connSock = msg.connSock;
    } else {
      // Fetch a new connection
      logger("globalCacheOps thread : Extracting new connection from pool");
      connSock = connectionPool.front();
      logger("globalCacheOps thread : Extracted new connection from pool, "
             "connSock : ",
             connSock);

      logger(
          "globalCacheOps thread : Adding to activeConnections : connSock : ",
          connSock);
      activeConnections.insert(connSock);
      connectionPool.pop_front();
    }

    int msgLength = msg.op.length();
    int writtenBytes = write(connSock, msg.op.c_str(), msgLength);

    if (writtenBytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Try again later, re-queue the message
        logger(
            "globalCacheOps thread : Got EAGAIN while writing to connSock : ",
            connSock, " requeuing");
        msg.partial = true;
        msg.connSock = connSock;
        GlobalCacheOpsQueue.enqueue(msg);
      } else {
        logger("globalCacheOps thread : Fatal write error, closing connection "
               "for connSock : ",
               connSock);
        onConnectionClose(connSock);
        if (msg.fd != -1) {
          logger("globalCacheOps thread : closing application query socket for "
                 "failed attempt : msg.fd : ",
                 msg.fd);
          close(msg.fd);
        } else {
          // Don't loose sync ops
          GlobalCacheOpsQueue.enqueue(msg);
        }
        continue;
      }
    } else if (writtenBytes == 0) {
      logger("globalCacheOps thread : Write returned zero (connection "
             "closed), connSock : ",
             connSock);
      if (msg.fd != -1) {
        logger("globalCacheOps thread : closing application query socket for "
               "failed attempt : msg.fd : ",
               msg.fd);
        close(msg.fd);
      } else {
        // Don't loose sync ops
        GlobalCacheOpsQueue.enqueue(msg);
      }
      onConnectionClose(connSock);
      continue;
    } else if (writtenBytes < msgLength) {
      logger("globalCacheOps thread : Partial write, requeuing for connSock : ",
             connSock);
      msg.op = msg.op.substr(writtenBytes);
      msg.partial = true;
      msg.connSock = connSock;
      GlobalCacheOpsQueue.enqueue(msg);
    }

    // Add in socketReqResMap for application queries
    if (msg.fd != -1) {
      logger("globalCacheOps thread : Adding to socketReqResMap for "
             "application queries, connSock : ",
             connSock, " application fd : ", msg.fd);
      socketReqResMap[connSock] = msg.fd;
    }
  }
}

void registerConnection(int conn) {
  logger("globalCacheOps thread : registering connection with GCP : conn : ",
         conn);
  int err;
  socklen_t socklen;

  // Get socket options
  logger("globalCacheOps thread : fetching socket options : conn : ", conn);
  getsockopt(conn, SOL_SOCKET, SO_ERROR, &err, &socklen);

  if (err != 0) {
    logger("globalCacheOps thread : Error while connecting to GCP : conn : ",
           conn);
    onConnectionClose(conn);
    return;
  }

  // Remove from epoll monitoring
  logger("globalCacheOps thread : removing from epoll monitoring : conn : ",
         conn);
  epoll_ctl(epollFd, EPOLL_CTL_DEL, conn, NULL);

  // Re-register for epoll monitor with EPOLLIN
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = conn;

  logger("globalCacheOps thread : adding connection : ", conn,
         " for monitoring by epoll");

  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, conn, &ev) == -1) {
    perror("globalCacheOps thread : epoll_ctl : conn");
    onConnectionClose(conn);
    return;
  }

  vector<QueryArrayElement> regMessage =
      connRegisterationMessage(configCommon::SENDER_CONNECTION_TYPE, lcpID);
  string regQuery = encoder(regMessage);

  // Write to GCP with registration message
  int writtenBytes = write(conn, regQuery.c_str(), regQuery.length());
  if (writtenBytes < regQuery.length()) {
    logger("globalCacheOps thread : Error while writing to GCP for connection "
           "registeration : conn : ",
           conn, " writtenBytes : ", writtenBytes);
    onConnectionClose(conn);
  }
}

void startNewConnPingTimeout() {
  logger("globalCacheOps thread : In startNewConnPingTimeout");
  // Start timer for PING timeout
  logger("globalCacheOps thread : arming timer");

  timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
    logger("globalCacheOps thread : Error while getting now");
    perror("globalCacheOps thread : now");
    // Don't throw error here
    // Let another iteration handle this check
    return;
  }

  struct itimerspec pingTimerValue;
  memset(&pingTimerValue, 0, sizeof(pingTimerValue));

  pingTimerValue.it_value.tv_sec = now.tv_sec + configLCP.PING_CHECK_INTERVAL;
  pingTimerValue.it_interval.tv_sec =
      0; // To be ran only one, after sending PING for all idle connections

  logger("globalCacheOps thread : pingTimerFdValue : ", pingTimerFdValue);
  if (timerfd_settime(pingTimerFdValue, TFD_TIMER_ABSTIME, &pingTimerValue,
                      NULL) == -1) {
    perror("globalCacheOps thread : timerfd_settime : pingTimer");
    // Don't throw error here
    // Let further iterations handle this
    return;
  }
}

void asyncConnect() {
  while (activeConnections.size() + connectionPool.size() <
         configLCP.MAX_GCP_CONNECTIONS) {
    logger("globalCacheOps thread : Establishing connection asynchronously");
    int connSockFd = establishConnection(configLCP.GCP_SERVER_IP.c_str(),
                                         configLCP.GCP_SERVER_PORT, true);

    if (connSockFd == -1) {
      logger("globalCacheOps thread : Error in establishConnection async, "
             "connSockFd : ",
             connSockFd);
      // This will be leaked from connection pool for this iteration
      // It will be re-tried in next health check attempt
      continue;
    }

    struct epoll_event ev;
    ev.events = EPOLLOUT | EPOLLET;
    ev.data.fd = connSockFd;

    logger("globalCacheOps thread : adding connection : ", connSockFd,
           " for monitoring by epoll");
    if (epoll_ctl(epollFd, EPOLL_CTL_ADD, connSockFd, &ev) == -1) {
      perror("globalCacheOps thread : epoll_ctl: connSockFd");
      // This will be leaked from connection pool for this iteration
      // It will be re-tried in next health check attempt
      close(connSockFd);
      continue;
    }

    activeConnections.insert(connSockFd);
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();
    newConnAndPingTimeoutMap[connSockFd] = now;
  }

  logger("globalCacheOps thread : Starting new conn timeout");
  startNewConnPingTimeout();
}

void onConnectionClose(int conn) {

  // Remove from monitoring
  logger("globalCacheOps thread : In connectionClose for conn : ", conn);
  logger("globalCacheOps thread : removing from epoll monitoring, conn : ",
         conn);

  if (epoll_ctl(epollFd, EPOLL_CTL_DEL, conn, nullptr) == -1) {
    logger("globalCacheOps thread : Error while removing from epoll "
           "monitoring, conn : ",
           conn);
    perror("globalCacheOps thread : Epoll_DEL");
  }

  close(conn);

  // Remove from activeConnections
  logger("globalCacheOps thread : Removing from activeConnections : conn : ",
         conn);
  activeConnections.erase(conn);

  // Remove from timeMap
  logger("globalCacheOps thread : Erasing from timeMap : conn : ", conn);
  timeMap.erase(conn);

  // Remove from socketReqResMap if applicable : Think on this
  logger("globalCacheOps thread : Erasing from socketReqResMap : conn : ",
         conn);
  socketReqResMap.erase(conn);

  // This is when GCP closes the connection
  // For other scenarios, it will not be present in pool
  auto it = connectionPool.begin();
  for (int i = 0; i < connectionPool.size(); i++) {
    if (connectionPool[i] == conn) {
      connectionPool.erase(it);
      break;
    }
    it++;
  }

  // Remove from newConnAndPingTimeoutMap
  newConnAndPingTimeoutMap.erase(conn);

  // Connection Asynchronously
  asyncConnect();
}

void poolHealthCheck() {
  logger("globalCacheOps thread : Checking pool health");
  // Loop over pool

  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  vector<QueryArrayElement> pingArray;
  QueryArrayElement pingMessage;
  pingMessage.type = "string";
  pingMessage.value = "PING";
  pingArray.push_back(pingMessage);

  string ping = encoder(pingArray);
  int poolSize = connectionPool.size();
  for (int i = 0; i < poolSize; i++) {
    int conn = connectionPool.front();
    connectionPool.pop_front();

    // Check timeMap
    auto timeMapData = timeMap.find(conn);
    time_t connLastActivityTimestamp = 0;
    if (timeMapData != timeMap.end()) {
      connLastActivityTimestamp = timeMapData->second;
    }

    // Send PING for idle connections
    if (now - connLastActivityTimestamp > configLCP.IDLE_CONNECTION_TIME) {
      logger("globalCacheOps thread : sending ping for idle conn : ", conn);
      // Write to connection
      int writtenBytes = write(conn, ping.c_str(), ping.length());
      logger("globalCacheOps thread : writtenBytes : ", writtenBytes,
             " to conn : ", conn);

      if (writtenBytes < ping.length()) {
        logger("globalCacheOps thread : unable to write pingMessage to conn : ",
               conn, " closing it.");
        onConnectionClose(conn);
        continue;
      }

      // Erase from timeMap
      timeMap.erase(conn);
      logger("globalCacheOps thread : removed from timeMap : conn : ", conn);

      // Add to activeConnections
      activeConnections.insert(conn);
      logger("globalCacheOps thread : added to activeConnections : conn : ",
             conn);

      logger("globalCacheOps thread : adding to newConnAndPingTimeoutMap for "
             "tracking "
             "irresponsive PINGs : conn : ",
             conn);
      newConnAndPingTimeoutMap[conn] = now;
    } else {
      connectionPool.push_back(conn);
    }
  }

  logger("globalCacheOps thread : Starting PING timeout");
  startNewConnPingTimeout();
}

void newConnAndPingHealthCheck() {
  // Close all irresponsive connections present in newConnAndPingTimeoutMap

  logger("globalCacheOps thread : In newConnAndPingHealthCheck");
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  logger("globalCacheOps thread : now : ", now,
         " newConnAndPingTimeoutMap size : ", newConnAndPingTimeoutMap.size());
  for (auto it = newConnAndPingTimeoutMap.begin();
       it != newConnAndPingTimeoutMap.end();) {
    if (now - it->second >= configLCP.PING_CHECK_INTERVAL * 1000) {
      int fd = it->first;
      it = newConnAndPingTimeoutMap.erase(it);
      onConnectionClose(fd);
    } else {
      ++it;
    }
  }
}

void globalCacheOps(
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue,
    int globalCacheThreadEventFd, string lcpId) {

  int timeout = -1;
  std::deque<ReadSocketMessage> readSocketQueue;
  std::deque<WriteSocketMessage> writeSocketQueue;
  bool poolHealthCheckEvent = false;
  bool newConnAndPingHealthCheckEvent = false;
  lcpID = lcpId;

  // Initialize epoll & add globalCacheThreadEventFd for monitoring
  // GCP connections + timerFd + eventFd + pingTimerFd
  int maxEpollEvent = configLCP.MAX_GCP_CONNECTIONS + 3;
  struct epoll_event ev, events[maxEpollEvent];

  epollFd = epoll_create1(0);
  if (epollFd == -1) {
    perror("globalCacheOps thread : epoll create error");
    exit(EXIT_FAILURE);
  }

  // Configure Edge triggered
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = globalCacheThreadEventFd;

  // Add globalCacheThreadEventFd for monitoring
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, globalCacheThreadEventFd, &ev) == -1) {
    perror("globalCacheOps thread : epoll_ctl globalCacheThreadEventFd ");
    exit(EXIT_FAILURE);
  }

  // Timer for Health check
  int timerFd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
  logger("globalCacheOps thread : timerFd : ", timerFd);

  if (timerFd == -1) {
    perror("globalCacheOps thread : timerFd");
    exit(EXIT_FAILURE);
  }

  struct itimerspec timerValue;
  timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
    logger("globalCacheOps thread : Error while getting now");
    perror("globalCacheOps thread : now");
    exit(EXIT_FAILURE);
  }

  memset(&timerValue, 0, sizeof(timerValue));
  timerValue.it_value.tv_sec =
      now.tv_sec + configLCP.CONNECTION_POOL_HEALTH_CHECK_INTERVAL;
  timerValue.it_interval.tv_sec =
      configLCP.CONNECTION_POOL_HEALTH_CHECK_INTERVAL;

  // Configure Edge triggered
  logger("globalCacheOps thread : Add timerFd for monitoring");
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = timerFd;

  // Add timerfd descriptor for monitoring
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, timerFd, &ev) == -1) {
    perror("globalCacheOps thread : epoll_ctl timerFd");
    exit(EXIT_FAILURE);
  }

  logger("globalCacheOps thread : arm timer");
  if (timerfd_settime(timerFd, TFD_TIMER_ABSTIME, &timerValue, NULL) == -1) {
    perror("globalCacheOps thread : timerfd_settime");
    exit(EXIT_FAILURE);
  }

  logger("globalCacheOps thread : Configuring PING timer");
  int pingTimerFd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
  logger("globalCacheOps thread : pingTimerFd : ", pingTimerFd);

  if (pingTimerFd == -1) {
    perror("globalCacheOps thread : pingTimerFd");
    exit(EXIT_FAILURE);
  }

  // Configure Edge triggered
  logger("globalCacheOps thread : Add pingTimerFd for monitoring");
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = pingTimerFd;

  // Add pingTimerFd descriptor for monitoring
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, pingTimerFd, &ev) == -1) {
    perror("globalCacheOps thread : epoll_ctl pingTimerFd");
    exit(EXIT_FAILURE);
  }
  pingTimerFdValue = pingTimerFd;

  // Establish connections with GCP Asynchronously
  logger(
      "globalCacheOps thread : Asynchronously establish connections with GCP");
  asyncConnect();

  logger("globalCacheOps thread : Starting eventLoop");
  while (1) {
    epollIO(epollFd, events, readSocketQueue, timeout, globalCacheThreadEventFd,
            timerFd, poolHealthCheckEvent, pingTimerFd,
            newConnAndPingHealthCheckEvent);
    readFromSocketQueue(readSocketQueue, writeSocketQueue);
    writeToApplicationSocket(writeSocketQueue);
    readFromConcurrentQueue(GlobalCacheOpsQueue);

    if (poolHealthCheckEvent) {
      poolHealthCheck();
      poolHealthCheckEvent = false;
    }

    if (newConnAndPingHealthCheckEvent) {
      newConnAndPingHealthCheck();
      newConnAndPingHealthCheckEvent = false;
    }

    if (readSocketQueue.size() || writeSocketQueue.size()) {
      timeout = 0;
    } else {
      timeout = -1;
    }
  }
}
