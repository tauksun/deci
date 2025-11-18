#include "group.hpp"
#include "../common/common.hpp"
#include "../common/logger.hpp"
#include "../common/responseDecoder.hpp"
#include "../common/wal.hpp"
#include "config.hpp"
#include "wal.hpp"
#include <deque>
#include <fstream>
#include <ios>
#include <pthread.h>
#include <string>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>

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
  int readyFds =
      epoll_wait(epollFd, events, configGCP.MAX_CONNECTIONS, timeout);
  if (readyFds == -1) {
    perror("Group : epoll_wait error");
    return -1;
  }

  for (int n = 0; n < readyFds; ++n) {
    if (events[n].data.fd == eventFd) {
      // Reading 1 Byte from eventFd (resets its counter)
      logger("Group : Reading eventFd for group ");
      while (true) {
        uint64_t counter;
        int count = read(eventFd, &counter, sizeof(counter));
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
    } else {
      // Read LCP sockets synchronization response
      // Push into readSocketQueue
      logger("Group : Adding to readSocketQueue, fd : ", events[n].data.fd);
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

    int readBytes = read(msg.fd, buf, configGCP.MAX_READ_BYTES);
    if (readBytes == 0) {
      // Connection closed by peer
      close(msg.fd);
      continue;
    } else if (readBytes < 0) {
      logger("Group : readBytes < 0, for fd : ", msg.fd);
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // No data now, skip
        logger("Group : read EAGAIN for fd : ", msg.fd);
      } else {
        // Other error, clean up
        logger("Group : Error while reading, closing fd : ", msg.fd);
        close(msg.fd);
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

        // Re-queue for event loop to read this till EAGAIN
        msg.data = "";
        msg.readBytes = 0;
        readSocketQueue.push_back(msg);
      } else {
        logger("Group : Successfully received sync response for fd : ", msg.fd);
        logger("Group : Adding back to connection pool, fd : ", msg.fd);
        auto fdLCP = fdLCPMap.find(msg.fd);
        string lcp = fdLCP->second;
        logger("Group : lcp for fd : ", msg.fd, " is : ", lcp);
        logger("Group : Pushing to LCP queue : ", lcp);
        auto lcpQueue = lcpQueueMap.find(lcp);
        lcpQueue->second.push_back(msg.fd);
        logger("Group : Successfully added fd :", msg.fd,
               " back to connection pool in lcp : ", lcp);

        // Re-queue for event loop to read this till EAGAIN
        msg.data = "";
        msg.readBytes = 0;
        readSocketQueue.push_back(msg);
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
        fdLCPMap.erase(msg.fd);
        lcpQueueMap[msg.lcp].pop_back();
        continue;
      }
      logger("Group : Added for monitoring by epoll, fd : ", msg.fd);

      // Respond back to Producer LCP socket
      WriteSocketMessage response;
      response.fd = msg.fd;
      response.response = ":1\r\n";
      logger("Group : Queuing LCP registration response to "
             "writeSocketQueue");
      writeSocketQueue.push_back(response);
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

        logger("Group : Removing from connection pool sock : ", sock,
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
    logger("Group : writeSocketQueue : fd : ", msg.fd, " msg : ", msg.query);
    int msgLength = msg.query.length();
    int writtenBytes = write(msg.fd, msg.query.c_str(), msgLength);

    if (writtenBytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Try again later, re-queue the whole message
        logger("Group : Got EAGAIN while writing to fd : ", msg.fd,
               " requeuing");
        writeSocketSyncQueue.push_back(msg);
      } else {
        logger("Group : Fatal write error, closing connection for fd: ",
               msg.fd);
        close(msg.fd);
      }
    } else if (writtenBytes == 0) {
      logger("Group : Write returned zero (connection closed?), fd: ", msg.fd);
      close(msg.fd);
    } else if (writtenBytes < msgLength) {
      logger("Group : Partial write, requeuing for fd : ", msg.fd);
      msg.query = msg.query.substr(writtenBytes);
      writeSocketSyncQueue.push_back(msg);
    }

    writeSocketSyncQueue.pop_front();
    pos++;
  }
}

void gWriteToSocketQueue(std::deque<WriteSocketMessage> &writeSocketQueue) {
  // Write few bytes & keep writing till whole content is written
  long pos = 0;
  long currentSize = writeSocketQueue.size();

  logger("Group : In gWriteToSocketQueue");

  while (pos < currentSize) {
    WriteSocketMessage response = writeSocketQueue.front();
    logger("Group : gWriteSocketQueue : fd : ", response.fd,
           " response : ", response.response);
    int responseLength = response.response.length();
    int writtenBytes =
        write(response.fd, response.response.c_str(), responseLength);

    if (writtenBytes < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Try again later, re-queue the whole message
        logger("Group : Got EAGAIN while writing to fd : ", response.fd,
               " requeuing");
        writeSocketQueue.push_back(response);
      } else {
        logger("Group : Fatal write error, closing connection for fd: ",
               response.fd);
        close(response.fd);
      }
    } else if (writtenBytes == 0) {
      logger("Group : Write returned zero (connection closed?), fd: ",
             response.fd);
      close(response.fd);
    } else if (writtenBytes < responseLength) {
      logger("Group : Partial write, requeuing for fd : ", response.fd);
      response.response = response.response.substr(writtenBytes);
      writeSocketQueue.push_back(response);
    }

    writeSocketQueue.pop_front();
    pos++;
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
           " eventFd : ", eventFd);
    thread writer(walWriter, std::ref(wal), groupName, std::ref(walQueue),
                  walEventFd);
    writer.detach();

    std::unordered_map<std::string, std::deque<int>> lcpQueueMap;

    // Maximum connections of all LCP combined in a group will be less than or
    // equal (if only single group is present) to configGCP.MAX_CONNECTIONS
    struct epoll_event ev, events[configGCP.MAX_CONNECTIONS];

    logger("Group : Creating epoll for group");
    int epollFd = epoll_create1(0);
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

    int timeout = -1;
    std::deque<ReadSocketMessage> readSocketQueue;
    std::deque<WriteSocketSyncMessage> writeSocketSyncQueue;
    std::deque<WriteSocketMessage> writeSocketQueue;
    unordered_map<int, string> fdLCPMap;

    logger("Group : Starting event loop");
    while (1) {
      if (gEpollIO(epollFd, eventFd, ev, events, timeout, readSocketQueue)) {
        return -1;
      }

      readFromSocketQueue(readSocketQueue, lcpQueueMap, fdLCPMap);
      readFromGroupConcurrentSyncQueue(
          queue, lcpQueueMap, fdLCPMap, writeSocketSyncQueue, writeSocketQueue,
          epollFd, ev, events, walQueue, walEventFd);
      writeToSocketSyncQueue(writeSocketSyncQueue);
      gWriteToSocketQueue(writeSocketQueue);

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

