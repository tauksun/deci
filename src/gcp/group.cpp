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
    std::unordered_map<std::string, std::deque<int>> &lcpQueueMap,
    std::unordered_map<int, std::string> &fdLCPMap) {
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
        logger("Adding back to connection pool, fd : ", msg.fd);
        auto fdLCP = fdLCPMap.find(msg.fd);
        string lcp = fdLCP->second;
        logger("lcp for fd : ", msg.fd, " is : ", lcp);
        logger("Pushing to LCP queue : ", lcp);
        auto lcpQueue = lcpQueueMap.find(lcp);
        lcpQueue->second.push_back(msg.fd);
        logger("Successfully added fd :", msg.fd,
               " back to connection pool in lcp : ", lcp);
      }
    }

    readSocketQueue.pop_front();
    pos++;
  }
}

void readFromGroupConcurrentSyncQueue(
    moodycamel::ConcurrentQueue<GroupConcurrentSyncQueueMessage> &queue,
    std::unordered_map<std::string, std::deque<int>> &lcpQueueMap,
    std::unordered_map<int, std::string> &fdLCPMap,
    std::deque<WriteSocketSyncMessage> &writeSocketSyncQueue,
    std::deque<WriteSocketMessage> &writeSocketQueue, int epollFd,
    struct epoll_event &ev, struct epoll_event *events) {

  for (;;) {

    GroupConcurrentSyncQueueMessage msg;
    bool found = queue.try_dequeue(msg);
    if (!found) {
      logger("No more messages to read from GroupConcurrentSyncQueueMessage.");
      break;
    }

    if (msg.connectionRegistration) {
      logger("Registering connection : ", msg.fd, " to lcp : ", msg.lcp);
      auto lcpQueue = lcpQueueMap.find(msg.lcp);
      if (lcpQueue == lcpQueueMap.end()) {
        logger("No queue is present for lcp : ", msg.lcp, " creating anew");
        lcpQueueMap[msg.lcp] = std::deque<int>();
        logger("Adding new connection to queue for lcp : ", msg.lcp);
        lcpQueueMap[msg.lcp].push_back(msg.fd);
      } else {
        logger(
            "Adding new connection to pool as queue already exists for lcp : ",
            msg.lcp);
        lcpQueueMap[msg.lcp].push_back(msg.fd);
      }

      fdLCPMap[msg.fd] = msg.lcp;

      // Add for monitoring by epollFd
      logger("Adding for monitoring by epoll, fd : ", msg.fd);

      // Configure Edge triggered
      ev.events = EPOLLIN | EPOLLET;
      ev.data.fd = msg.fd;

      // Add socket descriptor for monitoring
      if (epoll_ctl(epollFd, EPOLL_CTL_ADD, msg.fd, &ev) == -1) {
        perror("epoll_ctl ");
        logger("Failed to add for monitoring, fd : ", msg.fd);
        fdLCPMap.erase(msg.fd);
        lcpQueueMap[msg.lcp].pop_back();
        continue;
      }
      logger("Added for monitoring by epoll, fd : ", msg.fd);

      // Respond back to Producer LCP socket
      WriteSocketMessage response;
      response.fd = msg.fd;
      response.response = ":1\r\n";

      writeSocketQueue.push_back(response);
    } else {
      // Send sync message to other LCPs
      logger("Sending sync op to other lcps for producer lcp : ", msg.lcp);
      for (auto &pair : lcpQueueMap) {
        if (pair.first == msg.lcp) {
          logger("Skipping self LCP");
          continue;
        }

        logger("Sending sync to lcp : ", pair.first);
        if (!pair.second.size()) {
          logger("There is no connection available in queue of lcp : ",
                 pair.first);
          continue;
        }

        int sock = pair.second.front();
        WriteSocketSyncMessage syncMessage;
        syncMessage.fd = sock;
        syncMessage.query = msg.query;
        writeSocketSyncQueue.push_back(syncMessage);

        logger("Removing from connection pool sock : ", sock,
               " lcp : ", pair.first);
        pair.second.pop_front();
      }

      // Respond back to Producer LCP socket
      WriteSocketMessage response;
      response.fd = msg.fd;
      response.response = ":1\r\n";
      writeSocketQueue.push_back(response);
    }
  }
}

void writeToSocketSyncQueue(
    std::deque<WriteSocketSyncMessage> &writeSocketSyncQueue) {
  // Write few bytes & keep writing till whole content is written

  long pos = 0;
  long currentSize = writeSocketSyncQueue.size();

  while (pos < currentSize) {
    WriteSocketSyncMessage msg = writeSocketSyncQueue.front();
    write(msg.fd, msg.query.c_str(), msg.query.length());

    writeSocketSyncQueue.pop_front();
    pos++;
  }
}

void writeToSocketQueue(std::deque<WriteSocketMessage> &writeSocketQueue) {
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

int group(int eventFd,
          moodycamel::ConcurrentQueue<GroupConcurrentSyncQueueMessage> &queue,
          std::string &groupName) {

  logger("Group : ", groupName);
  std::unordered_map<std::string, std::deque<int>> lcpQueueMap;

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
  std::deque<WriteSocketSyncMessage> writeSocketSyncQueue;
  std::deque<WriteSocketMessage> writeSocketQueue;
  unordered_map<int, string> fdLCPMap;

  while (1) {
    if (epollIO(epollFd, eventFd, ev, events, timeout, readSocketQueue,
                groupName)) {

      return -1;
    }

    readFromSocketQueue(readSocketQueue, lcpQueueMap, fdLCPMap);
    readFromGroupConcurrentSyncQueue(queue, lcpQueueMap, fdLCPMap,
                                     writeSocketSyncQueue, writeSocketQueue,
                                     epollFd, ev, events);
    writeToSocketSyncQueue(writeSocketSyncQueue);
    writeToSocketQueue(writeSocketQueue);

    // Timeout
    if (readSocketQueue.size() || writeSocketQueue.size() ||
        writeSocketSyncQueue.size() || queue.size_approx()) {
      timeout = 0;
    } else {
      timeout = -1;
    }
  }

  return 0;
}

