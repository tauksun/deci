#include "server.hpp"
#include "../common/decoder.hpp"
#include "../common/encoder.hpp"
#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "../common/operate.hpp"
#include "config.hpp"
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <unordered_map>

unordered_map<string, CacheValue> cache;

void epollIO(int epollFd, int socketFd, struct epoll_event &ev,
             struct epoll_event *events, int timeout,
             std::deque<ReadSocketMessage> &readSocketQueue,
             int synchronizationEventFd) {

  logger("Server : In epollIO");
  int readyFds =
      epoll_wait(epollFd, events, configLCP.MAX_CONNECTIONS, timeout);
  if (readyFds == -1) {
    perror("epoll_wait error");
    exit(EXIT_FAILURE);
  }

  logger("Server : Looping over readyFds : ", readyFds);

  for (int n = 0; n < readyFds; ++n) {
    if (events[n].data.fd == socketFd) {
      while (true) {
        logger("Server : Accepting client connection");
        int connSock = accept(socketFd, NULL, NULL);

        if (connSock == -1) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // No more connections to accept
            logger("Server : Read EAGAIN on socketFd : ", socketFd,
                   " breaking from while");
            break;
          } else {
            logger("Server : Error while accepting connection on socketFd : ",
                   socketFd, " connSock : ", connSock);
            perror("Server : accept");
            continue;
          }
        }

        logger("Server : Connection accepted : ", connSock);
        logger("Server : Making connection non-blocking : ", connSock);
        if (makeSocketNonBlocking(connSock)) {
          perror("fcntl socketFd");
          close(connSock);
          continue;
        }

        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = connSock;

        logger("Server : Adding connection : ", connSock,
               " for monitoring by epoll");
        if (epoll_ctl(epollFd, EPOLL_CTL_ADD, connSock, &ev) == -1) {
          perror("epoll_ctl: connSock");
          close(connSock);
        }
      }
    } else if (events[n].data.fd == synchronizationEventFd) {
      // Reading 1 Byte from eventFd (resets its counter)
      logger("Server : Reading from synchronizationEventFd");
      uint64_t counter;
      read(synchronizationEventFd, &counter, sizeof(counter));
      logger("Server : Read synchronizationEventFd counter : ", counter);
    } else {
      // Add to readSocketQueue
      logger("Server : Adding to readSocketQueue : ", events[n].data.fd);
      ReadSocketMessage sock;
      sock.fd = events[n].data.fd;
      sock.data = "";
      sock.readBytes = 0;
      readSocketQueue.push_back(sock);
    }
  }
}

void readFromSocketQueue(
    std::deque<ReadSocketMessage> &readSocketQueue,
    std::deque<Operation> &operationQueue,
    std::deque<WriteSocketMessage> &writeSocketQueue,
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue,
    int globalCacheThreadEventFd) {
  // Read few bytes
  // If socket still has data, re-queue

  long pos = 0;
  long currentSize = readSocketQueue.size();
  bool pushedToGlobalCacheOpsQueue = false;

  logger("Server : In readFromSocketQueue");
  while (pos < currentSize) {
    char buf[1024];
    ReadSocketMessage msg = readSocketQueue.front();

    int readBytes = read(msg.fd, buf, configLCP.MAX_READ_BYTES);
    if (readBytes == 0) {
      // Connection closed by peer
      close(msg.fd);
      continue;
    } else if (readBytes < 0) {
      logger("Server : readBytes < 0, for fd : ", msg.fd);
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // No data now, skip
        logger("Server : EAGAIN for fd : ", msg.fd);
      } else {
        // Other error, clean up
        logger("Server : Error while reading from fd : ", msg.fd,
               " closing connection");
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
      // Parse the message here & push to operationQueue
      DecodedMessage parsed = decoder(msg.data);
      if (parsed.error.partial) {
        // If message is parsed partially, re-queue in readSocketQueue
        logger("Server : Partially parsed, re-queuing");
        readSocketQueue.push_back(msg);
      } else if (parsed.error.invalid) {
        logger("Server : Invalid message : ", msg.data);
        string res = "Invalid Message";
        WriteSocketMessage errorMessage;
        errorMessage.fd = msg.fd;
        errorMessage.response = encoder(&res, "error");
        writeSocketQueue.push_back(errorMessage);

        // Re-queue for event loop to read this till EAGAIN
        msg.data = "";
        msg.readBytes = 0;
        readSocketQueue.push_back(msg);
      } else {
        logger("Server : Successfully parsed");
        if (parsed.operation == "GGET" || parsed.operation == "GSET" ||
            parsed.operation == "GDEL" || parsed.operation == "GEXISTS") {
          // Send to another thread via eventfd & concurrent queue
          // Handle the Global cache lifecyle in that thread
          logger("Server : pushing msg to GlobalCacheOpsQueue Global for cache "
                 "operation");
          GlobalCacheOpMessage globalCacheMessage;
          globalCacheMessage.fd = msg.fd;
          globalCacheMessage.op = msg.data;
          GlobalCacheOpsQueue.enqueue(globalCacheMessage);
          pushedToGlobalCacheOpsQueue = true;
        } else {
          // If sync flag is set, queue in sync queue & use eventfd to wake up
          // another thread
          if (parsed.flag.sync) {
            logger(
                "Server : pushing msg to GlobalCacheOpsQueue sync operation");
            GlobalCacheOpMessage syncMessage;
            syncMessage.fd = -1; // No file descriptor for sync messages as this
                                 // is internal operation
            // Remove the sync flag from message to prevent recursive
            // synchronization
            string noSyncFlag = "0";
            syncMessage.op =
                msg.data.substr(0, msg.data.length() - 4) +
                encoder(&noSyncFlag,
                        "integer"); // Replace sync flag with 0 (:0\r\n)
            logger("Server : syncMessage : ", syncMessage.op);
            GlobalCacheOpsQueue.enqueue(syncMessage);
            pushedToGlobalCacheOpsQueue = true;
          }

          logger("Server : Pushing to operation queue for op : ",
                 parsed.operation);
          Operation op;
          op.fd = msg.fd;
          op.msg = parsed;
          operationQueue.push_back(op);
        }

        // Re-queue for event loop to read this till EAGAIN
        logger("Server : Requeing to readSocketQueue to read till EAGAIN");
        msg.data = "";
        msg.readBytes = 0;
        readSocketQueue.push_back(msg);
      }
    }

    readSocketQueue.pop_front();
    pos++;
  }

  // Trigger single event for n number of messages pushed
  if (pushedToGlobalCacheOpsQueue) {
    logger("Server : Triggering eventFd for GlobalCacheOpsQueue");
    uint64_t counter = 1;
    write(globalCacheThreadEventFd, &counter, sizeof(counter));
  }
}

void performOperation(std::deque<ReadSocketMessage> &readSocketQueue,
                      std::deque<Operation> &operationQueue,
                      std::deque<WriteSocketMessage> &writeSocketQueue) {

  long pos = 0;
  long currentSize = operationQueue.size();
  logger("Server : In performOperation");
  while (pos < currentSize) {
    // Perform operation

    WriteSocketMessage response;
    logger("Server : Performing operation");
    Operation op = operationQueue.front();
    logger("Server : Op : ", op.msg.operation);
    operate(op, response, cache);

    // Send Response
    response.fd = op.fd;
    writeSocketQueue.push_back(response);

    operationQueue.pop_front();
    pos++;
  }
}

void writeToSocketQueue(std::deque<WriteSocketMessage> &writeSocketQueue) {
  // Write few bytes & keep writing till whole content is written in further
  // iterations

  long pos = 0;
  long currentSize = writeSocketQueue.size();

  logger("Server : In writeToSocketQueue");

  while (pos < currentSize) {
    WriteSocketMessage response = writeSocketQueue.front();
    logger("Server : sending response to fd : ", response.fd,
           " response : ", response.response);
    int responseLength = response.response.length();
    int writtenBytes =
        write(response.fd, response.response.c_str(), responseLength);

    if (writtenBytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Try again later, re-queue the whole message
        logger("Server : Got EAGAIN while writing to fd : ", response.fd,
               " requeuing");
        writeSocketQueue.push_back(response);
      } else {
        logger("Server : Fatal write error, closing connection for fd: ",
               response.fd);
        close(response.fd);
      }
    } else if (writtenBytes == 0) {
      logger("Server : Write returned zero (connection closed?), fd: ",
             response.fd);
      close(response.fd);
    } else if (writtenBytes < responseLength) {
      logger("Server : Partial write, requeuing for fd : ", response.fd);
      response.response = response.response.substr(writtenBytes);
      writeSocketQueue.push_back(response);
    }

    writeSocketQueue.pop_front();
    pos++;
  }
}

void readFromSynchronizationQueue(
    moodycamel::ConcurrentQueue<Operation> &SynchronizationQueue,
    std::deque<WriteSocketMessage> &writeSocketQueue) {
  // Operate on n number of messages

  logger("Server : In readFromSynchronizationQueue");
  unsigned long queueSize = SynchronizationQueue.size_approx();
  for (int i = 0; i < min(queueSize, configLCP.MAX_SYNC_MESSAGES); i++) {
    WriteSocketMessage response;
    Operation msg;
    bool found = SynchronizationQueue.try_dequeue(msg);
    if (!found) {
      break;
    }
    operate(msg, response, cache);
    response.fd = msg.fd;
    writeSocketQueue.push_back(response);
  }
}

void server(
    const char *sock,
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue,
    moodycamel::ConcurrentQueue<Operation> &SynchronizationQueue,
    int globalCacheThreadEventFd, int synchronizationEventFd) {
  logger("Server : Unlinking sock : ", sock);
  unlink(sock);

  // Create
  logger("Server : Creating socket");
  int socketFd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socketFd == -1) {
    perror("Error while creating socket");
    exit(EXIT_FAILURE);
  }

  logger("Server : Making socket re-usable : ", socketFd);
  if (setsockopt(socketFd, SOL_SOCKET, SO_REUSEADDR, &configLCP.SOCKET_REUSE,
                 sizeof(configLCP.SOCKET_REUSE)) < 0) {
    perror("setsockopt(SO_REUSEADDR) failed");
    exit(EXIT_FAILURE);
  }

  // Bind
  struct sockaddr_un listener;
  listener.sun_family = AF_UNIX;
  strcpy(listener.sun_path, sock);
  logger("Server : Binding socket : ", socketFd);
  int bindResult =
      bind(socketFd, (struct sockaddr *)&listener, sizeof(listener));
  if (bindResult != 0) {
    perror("Error while binding to socket");
    exit(EXIT_FAILURE);
  }

  // Listen
  logger("Server : Starting listening on : ", socketFd);
  int listenResult = listen(socketFd, configLCP.MAX_CONNECTIONS);
  if (listenResult != 0) {
    perror("Error while listening on socket");
    exit(EXIT_FAILURE);
  }

  // Accept (E-Poll)
  struct epoll_event ev, events[configLCP.MAX_CONNECTIONS];

  logger("Server : Making server socket non-blocking : ", socketFd);
  if (makeSocketNonBlocking(socketFd)) {
    perror("fcntl socketFd");
    exit(EXIT_FAILURE);
  }

  int epollFd = epoll_create1(0);
  if (epollFd == -1) {
    perror("epoll create error");
    exit(EXIT_FAILURE);
  }

  // Configure Edge triggered
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = socketFd;

  // Add socket descriptor for monitoring
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, socketFd, &ev) == -1) {
    perror("epoll_ctl socketFd");
    exit(EXIT_FAILURE);
  }

  // Add synchronizationEventFd for monitoring
  // Read from it after receiving event to reset the Kernel counter
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = synchronizationEventFd;

  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, synchronizationEventFd, &ev) == -1) {
    perror("epoll_ctl synchronizationEventFd");
    exit(EXIT_FAILURE);
  }

  int timeout = 0;
  std::deque<ReadSocketMessage> readSocketQueue;
  std::deque<Operation> operationQueue;
  std::deque<WriteSocketMessage> writeSocketQueue;

  while (1) {

    /**
     * epoll_wait
     * Read from socket queue
     * perform operation on parsed messages
     * write response using socket queue
     *
     * */

    epollIO(epollFd, socketFd, ev, events, timeout, readSocketQueue,
            synchronizationEventFd);
    readFromSocketQueue(readSocketQueue, operationQueue, writeSocketQueue,
                        GlobalCacheOpsQueue, globalCacheThreadEventFd);
    performOperation(readSocketQueue, operationQueue, writeSocketQueue);
    readFromSynchronizationQueue(SynchronizationQueue, writeSocketQueue);
    writeToSocketQueue(writeSocketQueue);

    // If there are operations in read/write queue > keep timeout to be 0,
    // else -1(Infinity)
    if (readSocketQueue.size() || operationQueue.size() ||
        writeSocketQueue.size() || SynchronizationQueue.size_approx()) {
      timeout = 0;
    } else {
      timeout = -1;
    }

    logger("Server : Waiting for epoll with timeout : ", timeout);
  }
}
