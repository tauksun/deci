#include "synchronizationOps.hpp"
#include "../common/config.hpp"
#include "../common/decoder.hpp"
#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "config.hpp"
#include "connect.hpp"
#include "registration.hpp"
#include <deque>
#include <sys/epoll.h>
#include <unistd.h>

void epollIO(int epollFd, struct epoll_event *events,
             std::deque<ReadSocketMessage> &readSocketQueue, int timeout) {

  int readyFds =
      epoll_wait(epollFd, events, configLCP::MAX_SYNC_CONNECTIONS, timeout);

  if (readyFds == -1) {
    perror("cacheSynchronization thread : epoll_wait error");
    exit(EXIT_FAILURE);
  }

  for (int n = 0; n < readyFds; ++n) {
    // Add to readSocketQueue
    logger("cacheSynchronization thread : Adding to readSocketQueue : ",
           events[n].data.fd);
    ReadSocketMessage sock;
    sock.fd = events[n].data.fd;
    sock.data = "";
    sock.readBytes = 0;
    readSocketQueue.push_back(sock);
  }
}

void readFromSocketQueue(
    std::deque<ReadSocketMessage> &readSocketQueue,
    moodycamel::ConcurrentQueue<Operation> &SynchronizationQueue) {
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
      // Parse the message here & push to SynchronizationQueue
      DecodedMessage parsed = decoder(msg.data);
      if (parsed.error.partial) {
        // If message is parsed partially, re-queue in readSocketQueue
        logger("Partially parsed, re-queuing");
        readSocketQueue.push_back(msg);
      } else if (parsed.error.invalid) {
        logger("Invalid message : ", msg.data);
        close(msg.fd);
      } else {
        logger("Successfully parsed, adding to SynchronizationQueue");
        Operation op;
        op.fd = msg.fd;
        op.msg = parsed;
        SynchronizationQueue.enqueue(op);
      }
    }

    readSocketQueue.pop_front();
    pos++;
  }
}

void triggerEventfd(int synchronizationEventFd) {
  logger("triggerEventfd for synchronizationEventFd");
  uint64_t counter = 1;
  write(synchronizationEventFd, &counter, sizeof(counter));
  logger("synchronizationEventFd counter : ", counter);
}

void cacheSynchronization(
    moodycamel::ConcurrentQueue<Operation> &SynchronizationQueue,
    int synchronizationEventFd, string lcpId) {

  // Initialize epoll
  struct epoll_event ev, events[configLCP::MAX_SYNC_CONNECTIONS];

  int epollFd = epoll_create1(0);
  if (epollFd == -1) {
    perror("epoll create error");
    exit(EXIT_FAILURE);
  }

  // Establish connections with GCP
  for (int i = 0; i < configLCP::MAX_SYNC_CONNECTIONS; i++) {
    int connSockFd = establishConnection(configLCP::GCP_SERVER_IP,
                                         configLCP::GCP_SERVER_PORT);
    if (connSockFd == -1) {
      perror("cacheSynchronization thread: Failed to establishConnection with "
             "GCP");
      continue;
    }

    // Make fd non-blocking
    if (makeSocketNonBlocking(connSockFd)) {
      perror("cacheSynchronization thread : fcntl connSockFd");
      continue;
    }

    // Add for monitoring
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = connSockFd;

    // Synchronously register connection
    if (connectionRegistration(
            connSockFd, configCommon::RECEIVER_CONNECTION_TYPE, lcpId) == -1) {
      logger("cacheSynchronization thread: Failed to register connection with "
             "GCP");
      continue;
    }

    logger("cacheSynchronization thread : Successfully registered connection "
           "with GCP, connSockFd : ",
           connSockFd);

    logger("cacheSynchronization thread : Adding connection : ", connSockFd,
           " for monitoring by epoll");
    if (epoll_ctl(epollFd, EPOLL_CTL_ADD, connSockFd, &ev) == -1) {
      perror("cacheSynchronization thread : epoll_ctl: connSockFd");
      close(connSockFd);
    }
  }

  int timeout = -1;
  std::deque<ReadSocketMessage> readSocketQueue;
  while (1) {

    /**
     *
     * epoll wait
     * Read from sockets > parse cache operations
     * write to SynchronizationQueue & send trigger using eventfd
     * */

    epollIO(epollFd, events, readSocketQueue, timeout);
    readFromSocketQueue(readSocketQueue, SynchronizationQueue);
    triggerEventfd(synchronizationEventFd);

    // Set timeout to 0, if there is pending message in queue
    // else to -1 ( Infinity )
    if (readSocketQueue.size()) {
      timeout = 0;
    } else {
      timeout = -1;
    }
  }
}
