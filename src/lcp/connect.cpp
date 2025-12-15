#include "../common/logger.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <netinet/in.h>
#include <sys/socket.h>

int establishConnection(const char *serverIP, int port, bool async) {
  logger("Creating socket for establishing connection, async : ", async);

  int socketFd;
  if (async) {
    socketFd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  } else {
    socketFd = socket(AF_INET, SOCK_STREAM, 0);
  }

  if (socketFd == -1) {
    perror("Error while creating socketFd for establishing connection");
    return -1;
  }

  struct sockaddr_in server;
  server.sin_family = AF_INET;
  server.sin_addr.s_addr = inet_addr(serverIP);
  server.sin_port = htons(port);

  logger("Establishing connection with server : ", serverIP);
  int connection =
      connect(socketFd, (struct sockaddr *)&server, sizeof(server));
  if (connection == -1) {
    if (errno == EINPROGRESS) {
      logger("Async connection in progress, socketFd : ", socketFd);
      return socketFd;
    }
    perror("Error while establishing connection with server");
    return -1;
  }

  return socketFd;
}
