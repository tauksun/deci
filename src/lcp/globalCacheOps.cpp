#include "globalCacheOps.hpp"
#include "../common/common.hpp"
#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "../common/responseDecoder.hpp"
#include "config.hpp"
#include "connect.hpp"
#include <deque>
#include <sys/epoll.h>
#include <unistd.h>
#include <unordered_map>

void epollIO(int epollFd, struct epoll_event *events,
             std::deque<ReadSocketMessage> &readSocketQueue, int timeout,
             int globalCacheThreadEventFd) {

  int readyFds =
      epoll_wait(epollFd, events, configLCP::MAX_GCP_CONNECTIONS, timeout);

  if (readyFds == -1) {
    perror("globalCacheOps thread : epoll_wait error");
    exit(EXIT_FAILURE);
  }

  for (int n = 0; n < readyFds; ++n) {
    if (events[n].data.fd == globalCacheThreadEventFd) {
      // Reading 1 Byte from eventFd (resets its counter)
      logger("Reading from globalCacheThreadEventFd");
      uint64_t counter;
      read(globalCacheThreadEventFd, &counter, sizeof(counter));
      logger("Read globalCacheThreadEventFd counter : ", counter);
    } else {
      // Add to readSocketQueue
      logger("globalCacheOps thread : Adding to readSocketQueue : ",
             events[n].data.fd);
      ReadSocketMessage sock;
      sock.fd = events[n].data.fd;
      sock.data = "";
      sock.readBytes = 0;
      readSocketQueue.push_back(sock);
    }
  }
}

void readFromSocketQueue(std::deque<ReadSocketMessage> &readSocketQueue,
                         std::deque<WriteSocketMessage> &writeSocketQueue,
                         std::unordered_map<int, int> &socketReqResMap,
                         std::deque<int> &connectionPool) {
  // Read few bytes
  // If socket still has data, re-queue

  long pos = 0;
  long currentSize = readSocketQueue.size();

  while (pos < currentSize) {
    char buf[1024];
    ReadSocketMessage msg = readSocketQueue.front();

    int readBytes = read(msg.fd, buf, configLCP::MAX_READ_BYTES);
    if (readBytes == 0) {
      // Connection closed by peer
      close(msg.fd);
    } else if (readBytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // No data now, skip
      } else {
        // Other error, clean up
        close(msg.fd);
      }
    } else if (readBytes > 0) {
      msg.readBytes += readBytes;
      msg.data.append(buf, readBytes);
    }

    if (readBytes == configLCP::MAX_READ_BYTES) {
      // more data to read, Re-queue
      readSocketQueue.push_back(msg);
    } else {
      // Decode the response
      DecodedResponse decodedResponse = responseDecoder(msg.data);
      if (decodedResponse.error.partial) {
        // If response is decoded partially, re-queue in readSocketQueue
        logger("Partially decoded, re-queuing");
        readSocketQueue.push_back(msg);
      } else if (decodedResponse.error.invalid) {
        logger("Invalid response: ", msg.data);
        close(msg.fd);
      } else {
        logger("Successfully decoded response");
        // Write to Socket Queue for Application logic
        // Else > Add back to connection pool

        auto val = socketReqResMap.find(msg.fd);
        if (val != socketReqResMap.end()) {
          logger("globalCacheOps thread : adding to writeSocketQueue : ",
                 msg.fd);
          WriteSocketMessage response;
          response.fd = val->second; // Fetched from the hashmap
          response.response = msg.data;
          writeSocketQueue.push_back(response);
          logger("Erasing entry from socketReqResMap for : ", msg.fd);
          socketReqResMap.erase(msg.fd);
        }

        logger("globalCacheOps thread : adding ", msg.fd,
               " back to connection pool");
        connectionPool.push_back(msg.fd);
      }
    }

    readSocketQueue.pop_front();
    pos++;
  }
}

void writeToApplicationSocket(
    std::deque<WriteSocketMessage> &writeSocketQueue) {
  // Write few bytes & keep writing till whole content is written

  long pos = 0;
  long currentSize = writeSocketQueue.size();

  while (pos < currentSize) {
    WriteSocketMessage response = writeSocketQueue.front();
    write(response.fd, response.response.c_str(), response.response.length());

    writeSocketQueue.pop_front();
    pos++;
  }
}

void readFromConcurrentQueue(
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue,
    std::deque<int> &connectionPool, unordered_map<int, int> &socketReqResMap) {

  // check connectionPool > if available : send query to GCP
  for (int i = 0;; i++) {
    int isConnectionAvailable = connectionPool.size();
    if (!isConnectionAvailable) {
      logger("globalCacheOps thread : connection pool is empty");
      break;
    }

    GlobalCacheOpMessage msg;
    bool found = GlobalCacheOpsQueue.try_dequeue(msg);
    if (!found) {
      logger("GlobalCacheOpsQueue is empty");
      break;
    }

    // Write to a socket connection from connectionPool
    int connSock = connectionPool.front();
    write(connSock, msg.op.c_str(), msg.op.length());

    // Add in socketReqResMap for application queries
    if (msg.fd != -1) {
      socketReqResMap[connSock] = msg.fd;
    }

    connectionPool.pop_front();
  }
}

void globalCacheOps(
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue,
    int globalCacheThreadEventFd) {

  int timeout = -1;
  std::deque<int> connectionPool;
  std::unordered_map<int, int> socketReqResMap;
  std::deque<ReadSocketMessage> readSocketQueue;
  std::deque<WriteSocketMessage> writeSocketQueue;

  // Initialize epoll & add globalCacheThreadEventFd for monitoring
  struct epoll_event ev, events[configLCP::MAX_GCP_CONNECTIONS];

  int epollFd = epoll_create1(0);
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

  // Establish connections with GCP
  for (int i = 0; i < configLCP::MAX_GCP_CONNECTIONS; i++) {

    int connSockFd = establishConnection(configLCP::GCP_SERVER_IP,
                                         configLCP::GCP_SERVER_PORT);
    // Make fd non-blocking
    if (makeSocketNonBlocking(connSockFd)) {
      perror("globalCacheOps thread : fcntl connSockFd");
      continue;
    }

    // Add for monitoring
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = connSockFd;

    logger("globalCacheOps thread : adding connection : ", connSockFd,
           " for monitoring by epoll");
    if (epoll_ctl(epollFd, EPOLL_CTL_ADD, connSockFd, &ev) == -1) {
      perror("globalCacheOps thread : epoll_ctl: connSockFd");
      close(connSockFd);
      continue;
    }

    // Add to connection pool
    connectionPool.push_back(connSockFd);
  }

  /**
   * epoll_wait
   * readFromSocketQueue ( GCP response )
   * writeToApplicationSocket ( Application Global cache query response )
   * readFromConcurrentQueue > check pool > if available : send to GCP > Add
   * socketFd to socketReqResMap set timeout for epoll
   * */

  while (1) {
    epollIO(epollFd, events, readSocketQueue, timeout,
            globalCacheThreadEventFd);
    readFromSocketQueue(readSocketQueue, writeSocketQueue, socketReqResMap,
                        connectionPool);
    writeToApplicationSocket(writeSocketQueue);
    readFromConcurrentQueue(GlobalCacheOpsQueue, connectionPool,
                            socketReqResMap);

    if (readSocketQueue.size() || writeSocketQueue.size()) {
      timeout = 0;
    } else {
      timeout = -1;
    }
  }
}
