#include "wal.hpp"
#include "../common/logger.hpp"
#include "../common/wal.hpp"
#include "config.hpp"
#include <fstream>
#include <ios>
#include <string>
#include <sys/epoll.h>
#include <unistd.h>

int walEpollIO(int epollFd, int eventFd, struct epoll_event &ev,
               struct epoll_event *events, int timeout) {

  logger("WAL thead : In walEpollIO");
  int readyFds =
      epoll_wait(epollFd, events, configGCP.WAL_EPOLL_CONNECTIONS, timeout);
  if (readyFds == -1) {
    perror("WAL thread : epoll_wait error");
    return -1;
  }

  // There should only be one readyFd i.e., eventFd
  logger("WAL thread : Looping for readyFds : ", readyFds);
  for (int n = 0; n < readyFds; ++n) {
    if (events[n].data.fd == eventFd) {
      // Reading 1 Byte from eventFd (resets its counter)
      logger("WAL thread : Reading eventFd for group ");
      uint64_t counter;
      read(eventFd, &counter, sizeof(counter));
      logger("WAL thread : Read eventFd counter : ", counter);
    } else {
      logger("WAL thread : Invalid readyFd : ", events->data.fd);
      return -1;
    }
  }

  return 0;
}

void traverseQueueAndWriteToFile(
    std::fstream &wal, std::string groupName,
    moodycamel::ConcurrentQueue<std::string> &walQueue) {

  logger("WAL thread : In traverseQueueAndWriteToFile");
  int pos = 0;
  int queueSize = walQueue.size_approx();

  logger("WAL thread : walQueue size : ", queueSize);
  while (pos < queueSize) {

    string operation;
    walQueue.try_dequeue(operation);

    logger("WAL thread : Writing sync message to WAL file");

    if (!wal.is_open()) {
      logger("WAL thread : WAL file is not open, try opening");
      std::string groupWalFile = generateWalFileName(groupName);
      wal.open(groupWalFile, ios::app | ios::out);
      if (!wal.is_open()) {
        logger("WAL thread : Unable to open WAL file: ", groupWalFile);
        logger("WAL thread : This operation will not be logged in WAL file, "
               "operation : ",
               operation);
        continue;
      }
    }

    logger("WAL thread : Writing operation to wal file, op : ", operation);
    wal.write(operation.c_str(), operation.length());

    if (wal.fail()) {
      logger("WAL thread : Error while logging operation in WAL file, "
             "operation : ",
             operation);
    }

    pos++;
  }

  wal.flush();
}

void walWriter(std::fstream &wal, std::string groupName,
               moodycamel::ConcurrentQueue<std::string> &walQueue,
               int eventFd) {

  logger("WAL thread : started");
  struct epoll_event ev, events[configGCP.WAL_EPOLL_CONNECTIONS];

  logger("WAL thread : configGCP.WAL_EPOLL_CONNECTIONS : ",
         configGCP.WAL_EPOLL_CONNECTIONS);

  int epollFd = epoll_create1(0);
  if (epollFd == -1) {
    perror("WAL thread : epoll create error");
    exit(EXIT_FAILURE);
  }

  // Configure Edge triggered
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = eventFd;

  // Add socket descriptor for monitoring
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, eventFd, &ev) == -1) {
    perror("WAL thread : epoll_ctl eventFd");
    exit(EXIT_FAILURE);
  }

  int timeout = 0;

  logger("WAL thread : Starting event loop");
  while (1) {
    // Listen on epoll
    walEpollIO(epollFd, eventFd, ev, events, timeout);

    // Traverse ConcurrentQueue > write to file
    traverseQueueAndWriteToFile(wal, groupName, walQueue);

    // Timeout
    if (walQueue.size_approx()) {
      timeout = 0;
    } else {
      timeout = -1;
    }
    logger("WAL thread : timeout : ", timeout);
  }
}
