#include "group.hpp"
#include "../common/common.hpp"
#include "../common/encoder.hpp"
#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "../common/responseDecoder.hpp"
#include "../common/wal.hpp"
#include "config.hpp"
#include "wal.hpp"
#include <cstring>
#include <deque>
#include <fstream>
#include <ios>
#include <pthread.h>
#include <string>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

void checkLCPHealth();
void startPoolHealthCheck(string, int, int);
void startPingTimeoutTimer(string, int, int);
void onConnectionClose(int, string);
void pingTimerHandler(int);

static int epollFd;
int lcpHealthTimerFd;
std::unordered_map<string, time_t> lcpHealthMap;
std::unordered_map<string, std::unordered_map<int, time_t>> timeMap;
std::unordered_map<string, std::unordered_set<int>> activeConnectionsMap;
std::unordered_map<string, std::unordered_map<int, time_t>> pingTimeoutMap;

unordered_map<int, string> fdLCPMap;
std::unordered_map<std::string, std::deque<int>> lcpQueueMap;
static std::deque<WriteSocketMessage> writeSocketQueue;

// To extract lcp & pingFd info in epollIO on timerFd trigger
struct timerFdPingFdLCP {
  string lcp;
  int pingFd;
  int lcpRequiredConnections;
};
std::unordered_map<int, timerFdPingFdLCP> pingTimerFdLCPMap;

void checkLCPHealth() {
  logger("Group : In checkLCPHealth");
  // LCP removal ( when application scales down or lcp is down )
  // Check all structure which will be modified on LCP removal

  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  for (auto it = lcpHealthMap.begin(); it != lcpHealthMap.end();) {
    if (now - it->second > configGCP.MAX_LCP_HEALTH_STALE_TIME * 1000) {
      string lcp = it->first;
      logger("Group : lcp : ", lcp,
             " is xxx un-healthy xxx, clearing its structures");

      logger("Group : Removing from lcpHealthMap : lcp : ", lcp);
      it = lcpHealthMap.erase(it);

      // Close & remove each connection for this LCP from structures (
      // fdLCPMap, connection pool, activeConnections, timeMap ), Remove from
      // epoll monitoring

      logger("Group : Closing all connections for lcp : ", lcp);
      auto lqm = lcpQueueMap.find(lcp);
      if (lqm != lcpQueueMap.end()) {
        while (lqm->second.size()) {
          logger("Group : Closing connection :  ", lqm->second[0],
                 " : lcp : ", lcp);
          onConnectionClose(lqm->second[0], lcp);
        }
      }

      // Remove from lcpQueueMap
      logger("Group : Removing from lcpQueueMap : lcp : ", lcp);
      lcpQueueMap.erase(lcp);

      // Remove from pingTimeoutMap
      logger("Group : Removing from pingTimeoutMap : lcp : ", lcp);
      pingTimeoutMap.erase(lcp);

      // Remove from activeConnectionsMap
      logger("Group : Removing from activeConnectionsMap : lcp : ", lcp);
      activeConnectionsMap.erase(lcp);

      // Remove from timeMap
      logger("Group : Removing from timeMap : lcp : ", lcp);
      timeMap.erase(lcp);

    } else {
      logger("Group : lcp : ", it->first, " is healthy");
      it++;
    }
  }
}

void startPoolHealthCheck(string lcp, int pingFd, int lcpRequiredConnections) {
  logger("Group : In startPoolHealthCheck : lcp : ", lcp);

  // Loop over pool
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  vector<QueryArrayElement> pingArray;
  QueryArrayElement pingMessage;
  pingMessage.type = "string";
  pingMessage.value = "SYNC_PING_RESPONSE";
  pingArray.push_back(pingMessage);
  string ping = encoder(pingArray);

  logger("Group : Extracting connectionPool for lcp : ", lcp);
  auto cp = lcpQueueMap.find(lcp);
  if (cp == lcpQueueMap.end()) {
    logger("Group : Pool not found for lcp : ", lcp);
    return;
  }
  std::deque<int> &connectionPool = cp->second;
  int poolSize = connectionPool.size();

  logger("Group : Extracting timeMap for lcp : ", lcp);
  auto tMap = timeMap.find(lcp);
  if (tMap == timeMap.end()) {
    logger("Group : timeMap not found for lcp : ", lcp);
    return;
  }
  std::unordered_map<int, time_t> &connTimeMap = tMap->second;

  logger("Group : Extracting activeConnections for lcp : ", lcp);
  auto ac = activeConnectionsMap.find(lcp);
  if (ac == activeConnectionsMap.end()) {
    logger("Group : activeConnections not found for lcp : ", lcp);
    return;
  }
  std::unordered_set<int> &activeConnections = ac->second;

  logger("Group : Extracting pingTimeoutMap for lcp : ", lcp);
  auto pt = pingTimeoutMap.find(lcp);
  if (pt == pingTimeoutMap.end()) {
    logger("Group : pingTimeoutMap not found for lcp : ", lcp);
    return;
  }
  std::unordered_map<int, time_t> &ptMap = pt->second;

  for (int i = 0; i < poolSize; i++) {
    int conn = connectionPool.front();
    connectionPool.pop_front();

    // Check timeMap
    auto timeMapData = connTimeMap.find(conn);
    time_t connLastActivityTimestamp = 0;
    if (timeMapData != connTimeMap.end()) {
      connLastActivityTimestamp = timeMapData->second;
    }

    // Send PING for idle connections
    if (now - connLastActivityTimestamp > configGCP.IDLE_CONNECTION_TIME) {
      logger("Group thread : sending ping for idle conn : ", conn);
      // Write to connection
      int writtenBytes = write(conn, ping.c_str(), ping.length());
      logger("Group thread : writtenBytes : ", writtenBytes,
             " to conn : ", conn);

      if (writtenBytes < ping.length()) {
        logger("Group thread : unable to write pingMessage to conn : ", conn,
               " closing it.");
        onConnectionClose(conn, lcp);
        continue;
      }

      // Erase from timeMap
      connTimeMap.erase(conn);
      logger("Group thread : removed from timeMap : conn : ", conn);

      // Add to activeConnections
      activeConnections.insert(conn);
      logger("Group thread : added to activeConnections : conn : ", conn);

      logger("Group thread : adding to pingTimeoutMap for tracking "
             "irresponsive PING : conn : ",
             conn);
      ptMap[conn] = now;
    } else {
      connectionPool.push_back(conn);
    }
  }

  logger("Group thread : Starting PING timeout : lcp : ", lcp);
  startPingTimeoutTimer(lcp, pingFd, lcpRequiredConnections);
}

void startPingTimeoutTimer(string lcp, int pingFd, int lcpRequiredConnections) {
  logger("Group : In startPingTimeoutTimer : lcp : ", lcp,
         " : pingFd : ", pingFd);
  // Create timer
  logger("Group : Creating timer for PING timeouts for lcp : ", lcp);

  int timerFd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
  logger("Group : timerFd : ", timerFd, " : lcp : ", lcp);

  if (timerFd == -1) {
    perror("Group : timerFd");
    // let next iteration of PING handle this
    return;
  }

  struct itimerspec timerValue;
  timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
    logger("Group : Error while getting now");
    perror("Group : now");
    close(timerFd);
    return;
  }

  memset(&timerValue, 0, sizeof(timerValue));
  timerValue.it_value.tv_sec = now.tv_sec + configGCP.PING_CHECK_INTERVAL;
  timerValue.it_interval.tv_sec =
      0; // To be ran only one, after sending PING for all idle connections

  // Configure Edge triggered
  logger("Group : Add for epoll monitoring : timerFd : ", timerFd);
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = timerFd;

  // Add timerfd descriptor for monitoring
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, timerFd, &ev) == -1) {
    perror("Group : epoll_ctl timerFd");
    close(timerFd);
    return;
  }

  logger("Group : Adding to pingTimerFdLCPMap for lcp : ", lcp,
         " timerFd : ", timerFd);
  pingTimerFdLCPMap[timerFd] =
      timerFdPingFdLCP{lcp, pingFd, lcpRequiredConnections};

  // Arm timer
  logger("Group : Arming timer timerFd : ", timerFd);
  if (timerfd_settime(timerFd, TFD_TIMER_ABSTIME, &timerValue, NULL) == -1) {
    logger("Group : Error while arming timer : ", timerFd, " : lcp : ", lcp);
    perror("Group : timerfd_settime : pingTimer");
    // Don't throw error here
    // Let further iterations handle this
    close(timerFd);
    return;
  }
}

void pingTimerHandler(int pingTimerFd) {
  // Extract the lcp & pingFd info from pingTimerFdLCPMap

  logger("Group : In pingTimerHandler : pingTimerFd : ", pingTimerFd);
  auto pingTimerFdLCP = pingTimerFdLCPMap.find(pingTimerFd);
  if (pingTimerFdLCP == pingTimerFdLCPMap.end()) {
    logger("Group : Couldn't found pingTimerFd in pingTimerFdLCPMap : "
           "pingTimerFd : ",
           pingTimerFd);
    return;
  }

  // Loop over the pingTimeoutMap
  string lcp = pingTimerFdLCP->second.lcp;
  int lcpRequiredConnections = pingTimerFdLCP->second.lcpRequiredConnections;

  auto pingTimeoutVal = pingTimeoutMap.find(lcp);
  if (pingTimeoutVal == pingTimeoutMap.end()) {
    logger("Group : No value present in pingTimeoutMap for lcp : ", lcp);
    return;
  }

  logger("Group : Extracted pingTimeoutMap for lcp : ", lcp);

  // Close all irresponsive connections present in pingTimeoutMap
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  logger("Group : now : ", now,
         " pingTimeoutMap size : ", pingTimeoutVal->second.size());
  for (auto it = pingTimeoutVal->second.begin();
       it != pingTimeoutVal->second.end();) {
    if (now - it->second >= configGCP.PING_CHECK_INTERVAL * 1000) {
      int fd = it->first;
      it = pingTimeoutVal->second.erase(it);
      onConnectionClose(fd, lcp);
    } else {
      ++it;
    }
  }

  // Respond to the pingFd
  auto lcpConnPool = lcpQueueMap.find(lcp);
  if (lcpConnPool == lcpQueueMap.end()) {
    logger("Group : Couldn't find lcpConnPool in lcpQueueMap for lcp : ", lcp);
    return;
  }

  int activeConnections = 0;
  auto ac = activeConnectionsMap.find(lcp);
  if (ac == activeConnectionsMap.end()) {
    logger("Group : Couldn't find activeConnections for lcp : ", lcp);
  }
  activeConnections = ac->second.size();

  logger("Group : LCP : ", lcp, " activeConnections : ", activeConnections,
         " pool strength : ", lcpConnPool->second.size());

  WriteSocketMessage response;
  response.fd = pingTimerFdLCP->second.pingFd;

  int numberOfRequiredConnections =
      lcpRequiredConnections - (lcpConnPool->second.size() + activeConnections);
  string res = to_string(numberOfRequiredConnections);

  logger("Group : lcpRequiredConnections : ", lcpRequiredConnections);
  logger("Group : activeConnections : ", activeConnections);
  logger("Group : lcpConnPool size : ", lcpConnPool->second.size());
  logger("Group : lcp : ", lcp,
         " numberOfRequiredConnections : ", numberOfRequiredConnections);

  string pingResponse = encoder(&res, "integer");
  response.response = pingResponse;
  logger(
      "Group : Queuing LCP PING response to writeSocketQueue : pingResponse : ",
      pingResponse);
  writeSocketQueue.push_back(response);

  // Remove this pingTimerFd from epoll monitoring
  logger("Group : Removing from epoll monitoring : pingTimerFd : ",
         pingTimerFd);
  if (epoll_ctl(epollFd, EPOLL_CTL_DEL, pingTimerFd, nullptr) == -1) {
    logger("Group : Error while removing from epoll "
           "monitoring, pingTimerFd : ",
           pingTimerFd);
  }

  logger("Group : Closing pingTimerFd : ", pingTimerFd);
  pingTimerFdLCPMap.erase(pingTimerFd);
  close(pingTimerFd);
}

void onConnectionClose(int fd, string lcp) {
  logger("Group : In onConnectionClose : fd : ", fd, " lcp : ", lcp);

  // Remove from epoll monitoring
  logger("Group : Removing from epoll monitoring : fd : ", fd);
  if (epoll_ctl(epollFd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
    logger("Group : Error while removing from epoll "
           "monitoring, fd : ",
           fd);
  }

  // Close
  close(fd);

  // Remove from activeConnections
  auto ac = activeConnectionsMap.find(lcp);
  if (ac != activeConnectionsMap.end()) {
    logger("Group : Removing from activeConnections : lcp : ", lcp,
           " fd : ", fd);
    ac->second.erase(fd);
  }

  // Remove from timeMap
  auto tm = timeMap.find(lcp);
  if (tm != timeMap.end()) {
    logger("Group : Removing from timeMap : lcp : ", lcp, " fd : ", fd);
    tm->second.erase(fd);
  }

  // Remove from pingTimeoutMap
  auto ptm = pingTimeoutMap.find(lcp);
  if (ptm != pingTimeoutMap.end()) {
    logger("Group : Removing from pingTimeoutMap : lcp : ", lcp, " fd : ", fd);
    ptm->second.erase(fd);
  }

  // Remove from fdLCPMap
  logger("Group : Removing from fdLCPMap : fd : ", fd);
  fdLCPMap.erase(fd);

  // Remove from connection pool ( This can be present in pool, when LCP
  // shutdown & connection is closed from readFromSocketQueue)
  auto lcpq = lcpQueueMap.find(lcp);
  if (lcpq != lcpQueueMap.end()) {
    logger("Group : Removing from connection pool of lcp : ", lcp,
           " : fd : ", fd);

    auto it = lcpq->second.begin();
    for (int i = 0; i < lcpq->second.size(); i++) {
      if (lcpq->second[i] == fd) {
        lcpq->second.erase(it);
        break;
      } else {
        it++;
      }
    }
  }
}

void triggerEventfd(int walEventFd) {
  logger("Group : triggering walEventFd");
  uint64_t counter = 1;
  write(walEventFd, &counter, sizeof(counter));
  logger("Group : walEventFd counter : ", counter);
}

int gEpollIO(int epollFd, int eventFd, struct epoll_event &ev,
             struct epoll_event *events, int timeout,
             std::deque<ReadSocketMessage> &readSocketQueue) {

  logger("Group : In gEpollIO");
  logger("################");
  logger("epollFd : ", epollFd);
  logger("events : ", events);
  logger("configGCP.MAX_CONNECTIONS : ", configGCP.MAX_CONNECTIONS);
  logger("timeout : ", timeout);
  logger("################");
  int readyFds =
      epoll_wait(epollFd, events, configGCP.MAX_CONNECTIONS, timeout);
  if (readyFds == -1) {
    perror("Group : epoll_wait error");
    return -1;
  }

  for (int n = 0; n < readyFds; ++n) {
    int fd = events[n].data.fd;

    auto pingTimeoutFd = pingTimerFdLCPMap.find(fd);
    bool pingTimerEvent = pingTimeoutFd != pingTimerFdLCPMap.end();

    if ((fd == eventFd) || pingTimerEvent || (fd == lcpHealthTimerFd)) {
      // Reading 1 Byte from eventFd (resets its counter)
      logger("Group : Reading eventFd for group");
      while (true) {
        uint64_t counter;
        int count = read(fd, &counter, sizeof(counter));
        if (count == -1) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // No more data to read
            logger("Group : Drained eventFd");
            break;
          } else {
            perror("Group : Error while reading eventFd");
            break;
          }
        }
        logger("Group : Read eventFd counter : ", counter);
      }

      if (pingTimerEvent) {
        logger("Group : Initiating pingTimerHandler for timerFd : ", fd);
        pingTimerHandler(fd);
      }

      if (fd == lcpHealthTimerFd) {
        logger("Group : Initiating LCP Health Check");
        checkLCPHealth();
      }
    } else {
      // Read LCP sockets synchronization response
      // Push into readSocketQueue
      logger("Group : Adding to readSocketQueue, fd : ", fd);
      ReadSocketMessage msg;
      msg.fd = fd;
      msg.data = "";
      msg.readBytes = 0;
      readSocketQueue.push_back(msg);
    }
  }
  return 0;
}

void readFromSocketQueue(std::deque<ReadSocketMessage> &readSocketQueue) {
  // Read few bytes
  // If socket still has data, re-queue

  long pos = 0;
  long currentSize = readSocketQueue.size();

  while (pos < currentSize) {
    char buf[1024];
    ReadSocketMessage msg = readSocketQueue.front();
    readSocketQueue.pop_front();
    pos++;

    int readBytes = read(msg.fd, buf, configGCP.MAX_READ_BYTES);

    // Extract LCP for this fd
    logger("Group : Extracting LCP for fd : ", msg.fd);
    auto fdLcp = fdLCPMap.find(msg.fd);
    if (fdLcp == fdLCPMap.end()) {
      logger("Group : Couldn't find lcp for fd : ", msg.fd);
      close(msg.fd);
      continue;
    }

    if (readBytes == 0) {
      // Connection closed by peer
      onConnectionClose(msg.fd, fdLcp->second);
      continue;
    } else if (readBytes < 0) {
      logger("Group : readBytes < 0, for fd : ", msg.fd);
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // No data now, skip
        logger("Group : read EAGAIN for fd : ", msg.fd);
      } else {
        // Other error, clean up
        logger("Group : Error while reading, closing fd : ", msg.fd);
        onConnectionClose(msg.fd, fdLcp->second);
      }
    } else if (readBytes > 0) {
      msg.readBytes += readBytes;
      msg.data.append(buf, readBytes);
    }

    if (readBytes == configGCP.MAX_READ_BYTES) {
      // more data to read, Re-queue
      readSocketQueue.push_back(msg);
    } else if (readBytes > 0) {
      // Parse the message here & push to operationQueue
      DecodedResponse parsed = responseDecoder(msg.data);
      if (parsed.error.partial) {
        // If message is parsed partially, re-queue in readSocketQueue
        logger("Group : Partially parsed, re-queuing");
        logger("Group : TEMP : msg : ", msg.data, " readBytes : ", readBytes);
        readSocketQueue.push_back(msg);
      } else if (parsed.error.invalid) {
        logger("Group : Invalid message : ", msg.data, " fd : ", msg.fd);
        drainSocketSync(msg.fd);
      } else {
        drainSocketSync(msg.fd);
        logger("Group : Successfully received sync response for fd : ", msg.fd);
        logger("Group : Adding back to connection pool, fd : ", msg.fd);
        auto fdLCP = fdLCPMap.find(msg.fd);
        string lcp = fdLCP->second;

        // Add back to connection pool
        logger("Group : lcp for fd : ", msg.fd, " is : ", lcp);
        logger("Group : Pushing to LCP queue : ", lcp);
        auto lcpQueue = lcpQueueMap.find(lcp);
        lcpQueue->second.push_back(msg.fd);

        // Remove from activeConnectionsMap
        auto ac = activeConnectionsMap.find(lcp);
        if (ac != activeConnectionsMap.end()) {
          logger("Group : Erasing from activeConnectionsMap lcp : ", lcp,
                 " fd : ", msg.fd);
          ac->second.erase(msg.fd);
        }

        // Remove from pingTimeoutMap
        auto ptm = pingTimeoutMap.find(lcp);
        if (ptm != pingTimeoutMap.end()) {
          logger("Group : Erasing from pingTimeoutMap lcp : ", lcp,
                 " fd : ", msg.fd);
          ptm->second.erase(msg.fd);
        }

        // Update timeMap if LCP is found ( At the time of first connection
        // registeration, there won't be LCP, though the entry gets created
        // during first connection registeration )
        auto tMap = timeMap.find(lcp);
        if (tMap != timeMap.end()) {
          logger("Group : Updating timeMap for lcp : ", lcp, " fd : ", msg.fd);
          auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
          tMap->second[msg.fd] = now;
        }

        logger("Group : Successfully added fd :", msg.fd,
               " back to connection pool in lcp : ", lcp);

        logger("Group : lcp : ", lcp,
               " connection pool size : ", lcpQueue->second.size());
      }
    }
  }
}

void readFromGroupConcurrentSyncQueue(
    moodycamel::ConcurrentQueue<GroupConcurrentSyncQueueMessage> &queue,
    std::deque<WriteSocketSyncMessage> &writeSocketSyncQueue, int epollFd,
    struct epoll_event &ev, struct epoll_event *events,
    moodycamel::ConcurrentQueue<string> &walQueue, int walEventFd) {

  logger("Group : In readFromGroupConcurrentSyncQueue");

  bool isDataPushedToWALQueue = false;

  for (;;) {

    GroupConcurrentSyncQueueMessage msg;
    bool found = queue.try_dequeue(msg);
    if (!found) {
      logger("Group : No more messages to read from "
             "GroupConcurrentSyncQueueMessage.");
      break;
    }

    if (msg.connectionRegistration) {
      logger("Group : Registering connection : ", msg.fd,
             " to lcp : ", msg.lcp);
      auto lcpQueue = lcpQueueMap.find(msg.lcp);
      if (lcpQueue == lcpQueueMap.end()) {
        logger("Group : No queue is present for lcp : ", msg.lcp,
               " creating anew");
        lcpQueueMap[msg.lcp] = std::deque<int>();
        logger("Group : Adding new connection to queue for lcp : ", msg.lcp);
        lcpQueueMap[msg.lcp].push_back(msg.fd);

        // Add LCP in lcpHealthMap, timeMap, activeConnectionsMap,
        // pingTimeoutMap

        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

        logger(
            "Group : Adding lcp : ", msg.lcp,
            " to lcpHealthMap, timeMap, activeConnectionsMap, pingTimeoutMap");
        lcpHealthMap[msg.lcp] = now;
        timeMap[msg.lcp] = std::unordered_map<int, time_t>();
        activeConnectionsMap[msg.lcp] = std::unordered_set<int>();
        pingTimeoutMap[msg.lcp] = std::unordered_map<int, time_t>();
      } else {
        logger("Group : Adding new connection to pool as queue already exists "
               "for lcp : ",
               msg.lcp);
        lcpQueueMap[msg.lcp].push_back(msg.fd);
      }

      fdLCPMap[msg.fd] = msg.lcp;

      // Add for monitoring by epollFd
      logger("Group : Adding for monitoring by epoll, fd : ", msg.fd);

      // Configure Edge triggered
      ev.events = EPOLLIN | EPOLLET;
      ev.data.fd = msg.fd;

      // Add socket descriptor for monitoring
      if (epoll_ctl(epollFd, EPOLL_CTL_ADD, msg.fd, &ev) == -1) {
        perror("epoll_ctl ");
        logger("Group : Failed to add for monitoring, fd : ", msg.fd);
        onConnectionClose(msg.fd, msg.lcp);
        continue;
      }
      logger("Group : Added for monitoring by epoll, fd : ", msg.fd);

      // Respond back to Producer LCP socket
      // Using QueryArrayElement to send success response
      // This enables synchronizationOps thread from LCP to decode this response
      // using the existing readFromSocketQueue flow

      vector<QueryArrayElement> connResArray;
      QueryArrayElement connSuccessMessage;
      connSuccessMessage.type = "string";
      connSuccessMessage.value = "SYNC_CONN_ESTABLISHED";
      connResArray.push_back(connSuccessMessage);
      string connSuccessResponse = encoder(connResArray);

      WriteSocketMessage response;
      response.fd = msg.fd;
      response.response = connSuccessResponse;
      logger("Group : Queuing LCP registration response to "
             "writeSocketQueue");
      writeSocketQueue.push_back(response);
    } else if (msg.pingMessage) {
      // Update LCP health
      auto healthMap = lcpHealthMap.find(msg.lcp);
      logger("Group : Updating lcpHealthMap for lcp : ", msg.lcp);
      auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();
      lcpHealthMap[msg.lcp] = now;

      // Start Health Check for LCP
      logger("Group : starting pool health check for LCP : ", msg.lcp);
      startPoolHealthCheck(msg.lcp, msg.fd, msg.pingMessage);
    } else {
      // Send sync message to other LCPs
      logger("Group : Sending sync op in wal queue");
      walQueue.enqueue(msg.query);
      isDataPushedToWALQueue = true;

      logger("Group : Sending sync op to other lcps for producer lcp : ",
             msg.lcp);
      for (auto &pair : lcpQueueMap) {
        if (pair.first == msg.lcp) {
          logger("Group : Skipping self LCP");
          continue;
        }

        logger("Group : Sending sync to lcp : ", pair.first);
        if (!pair.second.size()) {
          logger("Group : There is no connection available in queue of lcp : ",
                 pair.first);
          continue;
        }

        int sock = pair.second.front();
        WriteSocketSyncMessage syncMessage;
        syncMessage.fd = sock;
        syncMessage.query = msg.query;
        writeSocketSyncQueue.push_back(syncMessage);

        // Remove from connection pool
        logger("Group : Removing from connection pool sock : ", sock,
               " lcp : ", pair.first);
        pair.second.pop_front();

        // Add to activeConnections
        logger("Group : Adding to activeConnections for lcp : ", msg.lcp,
               " fd : ", sock);
        auto ac = activeConnectionsMap.find(msg.lcp);
        if (ac != activeConnectionsMap.end()) {
          ac->second.insert(sock);
          logger("Group : Added to activeConnections : sock : ", sock);
        }
      }

      // Respond back to Producer LCP socket
      WriteSocketMessage response;
      response.fd = msg.fd;
      string res = "1";
      response.response = encoder(&res, "integer");
      writeSocketQueue.push_back(response);
    }
  }

  // Trigger WAL eventFd
  if (isDataPushedToWALQueue) {
    triggerEventfd(walEventFd);
  }
}

void writeToSocketSyncQueue(
    std::deque<WriteSocketSyncMessage> &writeSocketSyncQueue) {
  // Write few bytes & keep writing till whole content is written

  long pos = 0;
  long currentSize = writeSocketSyncQueue.size();

  logger("Group : In writeSocketQueue");
  while (pos < currentSize) {
    WriteSocketSyncMessage msg = writeSocketSyncQueue.front();
    writeSocketSyncQueue.pop_front();
    pos++;

    logger("Group : writeSocketQueue : fd : ", msg.fd, " msg : ", msg.query);
    int msgLength = msg.query.length();
    int writtenBytes = write(msg.fd, msg.query.c_str(), msgLength);

    logger("Group : Extracting lcp for fd : ", msg.fd);
    auto fdLcp = fdLCPMap.find(msg.fd);
    if (fdLcp == fdLCPMap.end()) {
      logger("Group : Couldn't find lcp for fd : ", msg.fd);
      close(msg.fd);
      continue;
    }

    if (writtenBytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Try again later, re-queue the whole message
        logger("Group : Got EAGAIN while writing to fd : ", msg.fd,
               " requeuing");
        writeSocketSyncQueue.push_back(msg);
      } else {
        logger("Group : Fatal write error, closing connection for fd: ",
               msg.fd);
        onConnectionClose(msg.fd, fdLcp->second);
      }
    } else if (writtenBytes == 0) {
      logger("Group : Write returned zero (connection closed?), fd: ", msg.fd);
      onConnectionClose(msg.fd, fdLcp->second);
    } else if (writtenBytes < msgLength) {
      logger("Group : Partial write, requeuing for fd : ", msg.fd);
      msg.query = msg.query.substr(writtenBytes);
      writeSocketSyncQueue.push_back(msg);
    }
  }
}

void gWriteToSocketQueue() {
  // Write few bytes & keep writing till whole content is written
  long pos = 0;
  long currentSize = writeSocketQueue.size();

  logger("Group : In gWriteToSocketQueue");

  while (pos < currentSize) {
    WriteSocketMessage response = writeSocketQueue.front();
    writeSocketQueue.pop_front();
    pos++;

    logger("Group : gWriteSocketQueue : fd : ", response.fd,
           " response : ", response.response);
    int responseLength = response.response.length();
    int writtenBytes =
        write(response.fd, response.response.c_str(), responseLength);

    logger("Group : writtenBytes : ", writtenBytes);

    if (writtenBytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Try again later, re-queue the whole message
        logger("Group : Got EAGAIN while writing to fd : ", response.fd,
               " requeuing");
        writeSocketQueue.push_back(response);
      } else {
        logger("Group : Fatal write error, closing connection for fd: ",
               response.fd);
        // This is LCP globalCacheOps connection
        // Closing it is enough here, it would be tracked via LCP
        close(response.fd);
      }
    } else if (writtenBytes == 0) {
      logger("Group : Write returned zero (connection closed?), fd: ",
             response.fd);
      // This is LCP globalCacheOps connection
      // Closing it is enough here, it would be tracked via LCP
      close(response.fd);
    } else if (writtenBytes < responseLength) {
      logger("Group : Partial write, requeuing for fd : ", response.fd);
      response.response = response.response.substr(writtenBytes);
      writeSocketQueue.push_back(response);
    }
  }
}

void lcpHealthTimer() {
  logger("Group : In lcpHealthTimer");

  lcpHealthTimerFd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
  logger("Group : lcpHealthTimerFd : ", lcpHealthTimerFd);

  if (lcpHealthTimerFd == -1) {
    perror("Group: lcpHealthTimerFd");
    exit(EXIT_FAILURE);
  }

  // Add to epoll monitoring
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = lcpHealthTimerFd;

  logger("Group : Adding lcpHealthTimerFd : ", lcpHealthTimerFd,
         " for monitoring by epoll");
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, lcpHealthTimerFd, &ev) == -1) {
    perror("Group : epoll_ctl: lcpHealthTimerFd");
    exit(EXIT_FAILURE);
  }

  logger("Group : Arming lcpHealthTimerFd : ", lcpHealthTimerFd);

  struct itimerspec timerValue;
  timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) == -1) {
    logger("Group : Error while getting now");
    perror("Group : now");
    exit(EXIT_FAILURE);
  }

  memset(&timerValue, 0, sizeof(timerValue));

  logger("Group : LCP_HEALTH_CHECK_INTERVAL : ",
         configGCP.LCP_HEALTH_CHECK_INTERVAL);
  timerValue.it_value.tv_sec = now.tv_sec + configGCP.LCP_HEALTH_CHECK_INTERVAL;
  timerValue.it_interval.tv_sec = configGCP.LCP_HEALTH_CHECK_INTERVAL;

  if (timerfd_settime(lcpHealthTimerFd, TFD_TIMER_ABSTIME, &timerValue, NULL) ==
      -1) {
    logger("Group : Error while arming lcpHealthTimerFd : ", lcpHealthTimerFd);
    perror("Group : timerfd_settime : lcpHealthTimerFd");
    exit(EXIT_FAILURE);
  }
}

int group(int eventFd,
          moodycamel::ConcurrentQueue<GroupConcurrentSyncQueueMessage> &queue,
          string groupName) {
  try {
    logger("Group : ", groupName, " thread started");

    string groupWalFile = generateWalFileName(groupName);
    logger("Group : Opening WAL file : ", groupWalFile);
    fstream wal(groupWalFile, ios::app | ios::out);
    if (!wal.is_open()) {
      logger("Group : Error while opening WAL file for group : ", groupName);
      pthread_exit(0);
    }

    logger("Group : Successfully opened wal file : ", groupWalFile,
           " in append mode");
    moodycamel::ConcurrentQueue<string> walQueue;
    int walEventFd = eventfd(0, EFD_NONBLOCK);

    logger("Group : Starting WAL writer thread for group : ", groupName,
           " walEventFd : ", walEventFd);
    thread writer(walWriter, std::ref(wal), groupName, std::ref(walQueue),
                  walEventFd);
    writer.detach();

    // Maximum connections of all LCP combined in a group will be less than or
    // equal (if only single group is present) to configGCP.MAX_CONNECTIONS
    struct epoll_event ev, events[configGCP.MAX_CONNECTIONS];

    logger("Group : Creating epoll for group");
    epollFd = epoll_create1(0);
    if (epollFd == -1) {
      perror("Group : epoll create error");
      // Don't throw error here as other groups could still be running
      return -1;
    }

    // Add group eventFd for monitoring
    // Configure Edge triggered
    logger("Group : Configuring epoll as Edge-triggered for group");
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = eventFd;

    if (epoll_ctl(epollFd, EPOLL_CTL_ADD, eventFd, &ev) == -1) {
      perror("Group : epoll_ctl eventFd");
      return -1;
    }

    lcpHealthTimer();

    int timeout = -1;
    std::deque<ReadSocketMessage> readSocketQueue;
    std::deque<WriteSocketSyncMessage> writeSocketSyncQueue;

    logger("Group : Starting event loop");
    while (1) {
      if (gEpollIO(epollFd, eventFd, ev, events, timeout, readSocketQueue)) {
        return -1;
      }

      readFromSocketQueue(readSocketQueue);
      readFromGroupConcurrentSyncQueue(queue, writeSocketSyncQueue, epollFd, ev,
                                       events, walQueue, walEventFd);
      writeToSocketSyncQueue(writeSocketSyncQueue);
      gWriteToSocketQueue();

      // Timeout
      if (readSocketQueue.size() || writeSocketQueue.size() ||
          writeSocketSyncQueue.size() || queue.size_approx()) {
        timeout = 0;
      } else {
        timeout = -1;
      }
    }
  } catch (const std::exception &ex) {
    logger("Group : Exception in group thread: ", ex.what());
    return -1;
  }

  return 0;
}
