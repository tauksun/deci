#include "group.hpp"
#include "../common/common.hpp"
#include "../common/logger.hpp"
#include "config.hpp"
#include <deque>
#include <string>
#include <sys/epoll.h>
#include <unistd.h>
#include <unordered_map>

int epollIO(int epollFd, int eventFd, struct epoll_event &ev,
            struct epoll_event *events, int timeout,
            std::deque<ReadSocketMessage> &readSocketQueue, string &groupName) {

  int readyFds =
      epoll_wait(epollFd, events, configGCP::MAX_CONNECTIONS, timeout);
  if (readyFds == -1) {
    perror("epoll_wait error");
    return -1;
  }

  for (int n = 0; n < readyFds; ++n) {
    if (events[n].data.fd == eventFd) {
      // Reading 1 Byte from eventFd (resets its counter)
      logger("Reading eventFd for group : ", groupName);
      uint64_t counter;
      read(eventFd, &counter, sizeof(counter));
      logger("Read eventFd counter : ", counter, " group : ", groupName);
    } else {
      // Read LCP sockets synchronization response
      // Push into readSocketQueue
      logger("Group : ", groupName,
             " : adding to readSocketQueue, fd : ", events[n].data.fd);
      ReadSocketMessage msg;
      msg.fd = events[n].data.fd;
      msg.data = "";
      msg.readBytes = 0;
      readSocketQueue.push_back(msg);
    }
  }
  return 0;
}

void readFromSocketQueue(
    std::deque<ReadSocketMessage> &readSocketQueue,
    std::unordered_map<std::string, std::deque<int> &> &lcpQueueMap) {
  // Read few bytes
  // If socket still has data, re-queue

  long pos = 0;
  long currentSize = readSocketQueue.size();

  while (pos < currentSize) {
    char buf[1024];
    ReadSocketMessage msg = readSocketQueue.front();

    int readBytes = read(msg.fd, buf, configGCP::MAX_READ_BYTES);
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

    if (readBytes == configGCP::MAX_READ_BYTES) {
      // more data to read, Re-queue
      readSocketQueue.push_back(msg);
    } else {
      // Parse the message here & push to operationQueue
      ParsedMessage parsed = msgParser(msg.data);
      if (parsed.error.partial) {
        // If message is parsed partially, re-queue in readSocketQueue
        logger("Partially parsed, re-queuing");
        readSocketQueue.push_back(msg);
      } else if (parsed.error.invalid) {
        logger("Invalid message : ", msg.data);
      } else {
        logger("Successfully received sync response for fd : ", msg.fd);
        logger("Adding back to connection pool");
      }
    }

    readSocketQueue.pop_front();
    pos++;
  }
}

int group(int eventFd,
          moodycamel::ConcurrentQueue<GroupConcurrentSyncQueueMessage> &queue,
          std::string &groupName) {

  logger("Group : ", groupName);
  std::unordered_map<std::string, std::deque<int> &> lcpQueueMap;

  // Maximum connections of all LCP combined in a group will be less than or
  // equal (if only single group is present) to configGCP::MAX_CONNECTIONS
  struct epoll_event ev, events[configGCP::MAX_CONNECTIONS];

  int epollFd = epoll_create1(0);
  if (epollFd == -1) {
    perror("epoll create error");
    // Don't throw error here as other groups could still be running
    return -1;
  }

  // Add group eventFd for monitoring
  // Configure Edge triggered
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = eventFd;

  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, eventFd, &ev) == -1) {
    perror("group epoll_ctl eventFd");
    return -1;
  }

  int timeout = -1;
  std::deque<ReadSocketMessage> readSocketQueue;

  while (1) {
    if (epollIO(epollFd, eventFd, ev, events, timeout, readSocketQueue,
                groupName)) {

      return -1;
    }

    readFromSocketQueue(readSocketQueue, lcpQueueMap);

  }

  return 0;
}
