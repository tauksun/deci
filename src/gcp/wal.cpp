#include "wal.hpp"
#include "../common/common.hpp"
#include "../common/decoder.hpp"
#include "../common/encoder.hpp"
#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "../common/wal.hpp"
#include "config.hpp"
#include <fstream>
#include <ios>
#include <pthread.h>
#include <string>
#include <sys/epoll.h>
#include <unistd.h>

DecodedMessage readSync(int connSock) {
  string msg;
  DecodedMessage decoded;

  logger("WAL thread : readSync : connSock : ", connSock);
  while (true) {
    int bufSize = 1023;
    char buf[bufSize];

    logger("WAL thread : readSync : Waiting on read for connSock : ", connSock);
    int readBytes = read(connSock, buf, bufSize);

    if (readBytes <= 0) {
      logger("WAL thread : readSync : Error while reading from connSock : ",
             connSock, " readBytes : ", readBytes);
      decoded.error.invalid = true;
      break;
    }

    msg.append(buf, readBytes);

    // check if the read is partial
    DecodedMessage temp = decoder(msg);

    logger("WAL thread : readSync : partial : ", temp.error.partial,
           " invalid : ", temp.error.invalid);
    if (!temp.error.partial || temp.error.invalid) {
      logger("WAL thread : readSync : Decoded temp message");
      decoded = temp;
      break;
    }
  }

  return decoded;
}

int walEpollIO(int epollFd, int eventFd, struct epoll_event &ev,
               struct epoll_event *events, int timeout) {

  logger("WAL writer : In walEpollIO");
  logger("WAL writer : epollFd : ", epollFd, " eventFd : ", eventFd,
         " timeout : ", timeout);

  assert(configGCP.WAL_EPOLL_CONNECTIONS > 0);
  assert(events != nullptr);
  assert(epollFd >= 0);

  int readyFds =
      epoll_wait(epollFd, events, configGCP.WAL_EPOLL_CONNECTIONS, timeout);
  if (readyFds == -1) {
    perror("WAL writer : epoll_wait error");
    return -1;
  }

  // There should only be one readyFd i.e., eventFd
  logger("WAL writer : Looping for readyFds : ", readyFds);
  for (int n = 0; n < readyFds; ++n) {
    if (events[n].data.fd == eventFd) {
      // Reading 1 Byte from eventFd (resets its counter)
      logger("WAL writer : Reading eventFd for group ");
      uint64_t counter;
      read(eventFd, &counter, sizeof(counter));
      logger("WAL writer : Read eventFd counter : ", counter);
    } else {
      logger("WAL writer : Invalid readyFd : ", events->data.fd);
      return -1;
    }
  }

  return 0;
}

void traverseQueueAndWriteToFile(
    std::fstream &wal, std::string groupName,
    moodycamel::ConcurrentQueue<std::string> &walQueue) {

  logger("WAL writer : In traverseQueueAndWriteToFile");
  int pos = 0;
  int queueSize = walQueue.size_approx();

  logger("WAL writer : walQueue size : ", queueSize);
  while (pos < queueSize) {

    string operation;
    walQueue.try_dequeue(operation);

    logger("WAL writer : Writing sync message to WAL file");

    if (!wal.is_open()) {
      logger("WAL writer : WAL file is not open, try opening");
      std::string groupWalFile = generateWalFileName(groupName);
      wal.open(groupWalFile, ios::app | ios::out);
      if (!wal.is_open()) {
        logger("WAL writer : Unable to open WAL file: ", groupWalFile);
        logger("WAL writer : This operation will not be logged in WAL file, "
               "operation : ",
               operation);
        continue;
      }
    }

    logger("WAL writer : Writing operation to wal file, op : ", operation);
    wal.write(operation.c_str(), operation.length());

    if (wal.fail()) {
      logger("WAL writer : Error while logging operation in WAL file, "
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

  logger("WAL writer : started");
  struct epoll_event ev, events[configGCP.WAL_EPOLL_CONNECTIONS];

  logger("WAL writer : configGCP.WAL_EPOLL_CONNECTIONS : ",
         configGCP.WAL_EPOLL_CONNECTIONS);

  int epollFd = epoll_create1(0);
  if (epollFd == -1) {
    perror("WAL writer : epoll create error");
    pthread_exit(0);
  }

  // Configure Edge triggered
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = eventFd;

  // Add socket descriptor for monitoring
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, eventFd, &ev) == -1) {
    perror("WAL writer : epoll_ctl eventFd");
    pthread_exit(0);
  }

  int timeout = 0;

  logger("WAL writer : Starting event loop");
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
    logger("WAL writer : timeout : ", timeout);
  }
}

void walReader(std::string group, int connSock) {
  logger("WAL reader : Group : ", group, " connSock : ", connSock);
  std::string ops;
  long seeker;

  std::string groupWalFile = generateWalFileName(group);
  fstream wal(groupWalFile, ios::in);
  if (!wal.is_open()) {
    logger("WAL reader : Unable to open WAL file: ", groupWalFile);
    close(connSock);
    pthread_exit(0);
  }

  // -------------------------- //
  // Synchronous communication //
  // -------------------------- //

  logger("WAL reader : Making socket blocking for Synchronous communication, "
         "connSock : ",
         connSock);
  makeSocketBlocking(connSock);

  // Write successful connection established to LCP for WAL SYNC
  string msg = "1";
  string response = encoder(&msg, "integer");
  int writtenBytes = 0;
  int responseLength = response.length();

  logger("WAL reader : Responding with success for connection established");
  if (writeSync(response, connSock) == -1) {
    logger("WAL reader : Write error : Closing connSock : ", connSock,
           " : Exiting failure");
    close(connSock);
    pthread_exit(0);
  };

  // Read Start/Resume command from LCP
  logger("WAL reader : Waiting for operation command from LCP, connSock : ",
         connSock);
  DecodedMessage decoded = readSync(connSock);
  if (decoded.error.invalid) {
    logger("WAL reader : Invalid message on connSock : ", connSock);
    close(connSock);
    pthread_exit(0);
  }

  // Extract the initial operation ( START / RESUME )

  logger("WAL reader : Initial key : ", decoded.key);
  if (decoded.key == "RESUME") {
    seeker = stol(decoded.value);
    logger("WAL reader : Seek file till: ", seeker,
           " for connSock : ", connSock);
    wal.seekg(seeker);
    if (wal.fail()) {
      logger("WAL reader : Seeker failed > Reading from 0");
    } else {
      logger("WAL reader : Seek success for connSock : ", connSock);
    }
  }

  // Read WAL as per seeker
  // Wait for LCP response : continue/stop accordingly

  QueryArrayElement op;
  op.value = "GWALSYNC";
  op.type = "string";

  logger("WAL reader : Starting reading & communication loop");

  while (true) {
    int walReadSize = 1023;
    char walBuf[walReadSize];

    logger("WAL reader : Reading from wal log file : ", groupWalFile);
    wal.read(walBuf, walReadSize);
    std::streamsize bytesRead = wal.gcount();

    logger("WAL reader : bytesRead : ", bytesRead);

    if (bytesRead == 0) {
      logger("WAL reader : Read failed for connSock : ", connSock);
      if (wal.eof()) {
        logger("WAL reader : EOF reached for connSock : ", connSock);
        vector<QueryArrayElement> responseArray;

        QueryArrayElement msg;
        msg.value = "EOF";
        msg.type = "string";

        responseArray.push_back(op);
        responseArray.push_back(msg);
        string response = encoder(responseArray);

        logger("WAL reader : Writing EOF to connSock : ", connSock);
        if (writeSync(response, connSock) == -1) {
          logger("WAL reader : Write error : Closing connSock : ", connSock,
                 " : Exiting failure");
          close(connSock);
          pthread_exit(0);
        };

        close(connSock);
        pthread_exit(0);
      } else if (wal.fail()) {
        logger("WAL reader : Closing connection, connSock : ", connSock);
        close(connSock);
        pthread_exit(0);
      }
    }

    // Write to LCP
    string walLog;
    walLog.append(walBuf, bytesRead);
    logger("WAL reader : Writing wal log to connSock : ", connSock);
    if (writeSync(walLog, connSock) == -1) {
      logger("WAL reader : Write error : Closing connSock : ", connSock,
             " : Exiting failure");
      close(connSock);
      pthread_exit(0);
    };

    // Wait for LCP response
    DecodedMessage lcpResponse = readSync(connSock);
    logger("WAL reader : lcpResponse.key : ", lcpResponse.key);
    if (lcpResponse.error.invalid) {
      logger("WAL reader : Received invalid LCP response on connSock : ",
             connSock, " Exiting failure");
      pthread_exit(0);
    }

    if (lcpResponse.key == "STOP") {
      logger("WAL reader : LCP requested STOP, connSock : ", connSock);
      close(connSock);
      pthread_exit(0);
    }
  }
}
