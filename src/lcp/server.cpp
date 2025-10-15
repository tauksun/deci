#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "config.hpp"
#include <cstdio>
#include <cstdlib>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

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

  // Instead of using threads 
  // Use non-blocking read with Edge triggered Epoll
  // Maintain a FIFO queue (using  Linked list)
  // Loop through the readyFds -> If not already present in the queue 
  // -> Add them -> Loop through the queue -> Process each client socket 
  // i.e., read 1023 bytes & if more data is present -> Add to the queue
  // Once the queue is processed -> Re-check epoll_wait & repeat.

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

  while (1) {
    // Wait time is infinite
    int readyFds = epoll_wait(epollFd, events, configLCP::MAXCONNECTIONS, -1);
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
        // Handle connection
      }
    }
  }
}
