#include "globalCacheOps.hpp"
#include "../common/common.hpp"
#include "../common/config.hpp"
#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "../common/responseDecoder.hpp"
#include "config.hpp"
#include "connect.hpp"
#include "registration.hpp"
#include <deque>
#include <sys/epoll.h>
#include <unistd.h>
#include <unordered_map>

void epollIO(int epollFd, struct epoll_event *events,
             std::deque<ReadSocketMessage> &readSocketQueue, int timeout,
             int globalCacheThreadEventFd) {
  logger("globalCacheOps thread : In epollIO");
  int readyFds =
      epoll_wait(epollFd, events, configLCP.MAX_GCP_CONNECTIONS, timeout);

  if (readyFds == -1) {
    perror("globalCacheOps thread : epoll_wait error");
    exit(EXIT_FAILURE);
  }

  logger("globalCacheOps thread : Looping on readyFds : ", readyFds);
  for (int n = 0; n < readyFds; ++n) {
    if (events[n].data.fd == globalCacheThreadEventFd) {
      // Reading 1 Byte from eventFd (resets its counter)
      logger("globalCacheOps thread : Reading from globalCacheThreadEventFd");
      while (true) {
        uint64_t counter;
        int count = read(globalCacheThreadEventFd, &counter, sizeof(counter));
        if (count == -1) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // No more data to read
            logger("globalCacheOps thread : Drained globalCacheThreadEventFd");
            break;
          } else {
            perror("globalCacheOps thread : Error while reading "
                   "globalCacheThreadEventFd");
            break;
          }
        }
        logger(
            "globalCacheOps thread : Read globalCacheThreadEventFd counter : ",
            counter);
      }
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

  logger("globalCacheOps thread : In readFromSocketQueue");
  long pos = 0;
  long currentSize = readSocketQueue.size();

  while (pos < currentSize) {
    char buf[1024];
    ReadSocketMessage msg = readSocketQueue.front();

    int readBytes = read(msg.fd, buf, configLCP.MAX_READ_BYTES);
    logger("globalCacheOps thread : Read ", readBytes,
           " bytes from fd : ", msg.fd);
    if (readBytes == 0) {
      // Connection closed by peer
      logger("No bytes read, closing connection for fd : ", msg.fd);
      close(msg.fd);
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
        close(msg.fd);
      }
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
        close(msg.fd);
      } else {
        logger(
            "globalCacheOps thread : Successfully decoded response for fd : ",
            msg.fd);
        drainSocketSync(msg.fd);

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
          logger("globalCacheOps thread : Erasing entry from socketReqResMap "
                 "for : ",
                 msg.fd);
          socketReqResMap.erase(msg.fd);
        }

        logger("globalCacheOps thread : adding ", msg.fd,
               " back to connection pool");
        connectionPool.push_back(msg.fd);
        logger("globalCacheOps thread : connectionPool size : ",
               connectionPool.size());
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

  logger("globalCacheOps thread : In writeToApplicationSocket");

  while (pos < currentSize) {
    WriteSocketMessage response = writeSocketQueue.front();
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

    writeSocketQueue.pop_front();
    pos++;
  }
}

void readFromConcurrentQueue(
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue,
    std::deque<int> &connectionPool, unordered_map<int, int> &socketReqResMap) {

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
    bool extractedFromConnectionPool = false;
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
      extractedFromConnectionPool = true;
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
        close(connSock);
        continue;
      }
    } else if (writtenBytes == 0) {
      logger("globalCacheOps thread : Write returned zero (connection "
             "closed), connSock : ",
             connSock);
      close(connSock);
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

    logger("globalCacheOps thread : extractedFromConnectionPool : ",
           extractedFromConnectionPool);
    if (extractedFromConnectionPool) {
      logger("globalCacheOps thread : Poping from connection pool");
      connectionPool.pop_front();
    }
  }
}

void globalCacheOps(
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue,
    int globalCacheThreadEventFd, string lcpId) {

  int timeout = -1;
  std::deque<int> connectionPool;
  std::unordered_map<int, int> socketReqResMap;
  std::deque<ReadSocketMessage> readSocketQueue;
  std::deque<WriteSocketMessage> writeSocketQueue;

  // Initialize epoll & add globalCacheThreadEventFd for monitoring
  struct epoll_event ev, events[configLCP.MAX_GCP_CONNECTIONS];

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
  for (int i = 0; i < configLCP.MAX_GCP_CONNECTIONS; i++) {

    int connSockFd = establishConnection(configLCP.GCP_SERVER_IP.c_str(),
                                         configLCP.GCP_SERVER_PORT);

    // Synchronously register connection
    if (connectionRegistration(connSockFd, configCommon::SENDER_CONNECTION_TYPE,
                               lcpId) == -1) {
      logger("globalCacheOps thread : Failed to register connection with "
             "GCP");
      continue;
    }

    logger("globalCacheOps thread : Successfully registered connection "
           "with GCP, connSockFd : ",
           connSockFd);

    // Make fd non-blocking
    logger("globalCacheOps thread : Making non-blocking connSockFd : ",
           connSockFd);
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
    logger("globalCacheOps thread : Adding to connection pool, fd : ",
           connSockFd);
    connectionPool.push_back(connSockFd);
  }

  /**
   * epoll_wait
   * readFromSocketQueue ( GCP response )
   * writeToApplicationSocket ( Application Global cache query response )
   * readFromConcurrentQueue > check pool > if available : send to GCP > Add
   * socketFd to socketReqResMap set timeout for epoll
   * */

  logger("globalCacheOps thread : Starting eventLoop");
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
