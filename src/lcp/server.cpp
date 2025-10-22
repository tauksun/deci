#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "config.hpp"
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

struct ReadSocketMessage {
  int fd;
  int readBytes = 0;
  std::string data = "";
};

struct Operation {
  int fd;
  std::string op = "";
};

struct WriteSocketMessage {
  int fd;
  int writtenBytes = 0;
  std::string response = "";
};

std::string msgParser(std::string msg) { return msg; }

void epollIO(int epollFd, int socketFd, struct epoll_event &ev,
             struct epoll_event *events, int timeout,
             std::deque<ReadSocketMessage> &readSocketQueue) {

  int readyFds =
      epoll_wait(epollFd, events, configLCP::MAXCONNECTIONS, timeout);
  if (readyFds == -1) {
    perror("epoll_wait error");
    exit(EXIT_FAILURE);
  }

  for (int n = 0; n < readyFds; ++n) {
    if (events[n].data.fd == socketFd) {
      logger("Accepting client connection");
      int connSock = accept(socketFd, NULL, NULL);
      if (connSock == -1) {
        perror("connSock accept");
        // Don't throw error
        continue;
      }

      logger("Connection accepted");
      logger("Making connection non-blocking");
      if (makeSocketNonBlocking(connSock)) {
        perror("fcntl socketFd");
        close(connSock);
        continue;
      }

      ev.events = EPOLLIN | EPOLLET;
      ev.data.fd = connSock;

      logger("Adding connection for monitoring by epoll");
      if (epoll_ctl(epollFd, EPOLL_CTL_ADD, connSock, &ev) == -1) {
        perror("epoll_ctl: connSock");
        close(connSock);
      }
    } else {
      // Add to readSocketQueue
      logger("Adding to readSocketQueue");
      ReadSocketMessage sock;
      sock.fd = events[n].data.fd;
      sock.data = "";
      sock.readBytes = 0;
      readSocketQueue.push_back(sock);
    }
  }
}

void readFromSocketQueue(std::deque<ReadSocketMessage> &readSocketQueue,
                         std::deque<Operation> &operationQueue) {
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
      // Parse the message here & push to operationQueue
      Operation op;
      op.fd = msg.fd;
      op.op = msgParser(msg.data);
      operationQueue.push_back(op);
    }

    readSocketQueue.pop_front();
    pos++;
  }
}

void server(const char *sock) {
  logger("Unlinking sock");
  unlink(sock);

  // Create
  logger("Creating socket");
  int socketFd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socketFd == -1) {
    perror("Error while creating socket");
    exit(EXIT_FAILURE);
  }

  // Bind
  struct sockaddr_un listener;
  listener.sun_family = AF_UNIX;
  strcpy(listener.sun_path, sock);
  logger("Binding socket");
  int bindResult =
      bind(socketFd, (struct sockaddr *)&listener, sizeof(listener));
  if (bindResult != 0) {
    perror("Error while binding to socket");
    exit(EXIT_FAILURE);
  }

  logger("Making socket re-usable");
  if (setsockopt(socketFd, SOL_SOCKET, SO_REUSEADDR, &configLCP::SOCKET_REUSE,
                 sizeof(configLCP::SOCKET_REUSE)) < 0) {
    perror("setsockopt(SO_REUSEADDR) failed");
    exit(EXIT_FAILURE);
  }

  // Listen
  logger("Starting listening");
  int listenResult = listen(socketFd, configLCP::MAXCONNECTIONS);
  if (listenResult != 0) {
    perror("Error while listening on socket");
    exit(EXIT_FAILURE);
  }

  // Accept (E-Poll)
  struct epoll_event ev, events[configLCP::MAXCONNECTIONS];

  logger("Making server socket non-blocking");
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

    epollIO(epollFd, socketFd, ev, events, timeout, readSocketQueue);
    readFromSocketQueue(readSocketQueue, operationQueue);

  }
}
