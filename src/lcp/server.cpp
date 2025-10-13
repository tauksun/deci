#include "config.hpp"
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

void server(char *sock) {
  unlink(sock);

  // Create
  int socketFd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socketFd == -1) {
    perror("Error while creating socket");
    exit(EXIT_FAILURE);
  }

  // Bind
  struct sockaddr_un listener;
  listener.sun_family = AF_UNIX;
  strcpy(listener.sun_path, sock);
  int bindResult =
      bind(socketFd, (struct sockaddr *)&listener, sizeof(listener));
  if (bindResult != 0) {
    perror("Error while binding to socket");
    exit(EXIT_FAILURE);
  }

  if (setsockopt(socketFd, SOL_SOCKET, SO_REUSEADDR, &configLCP::SOCKET_REUSE,
                 sizeof(configLCP::SOCKET_REUSE)) < 0) {
    perror("setsockopt(SO_REUSEADDR) failed");
    exit(EXIT_FAILURE);
  }

  // Listen
  int listenResult = listen(socketFd, configLCP::MAXCONNECTIONS);
  if (listenResult != 0) {
    perror("Error while listening on socket");
    exit(EXIT_FAILURE);
  }

  // Accept (E-Poll)
  
}
