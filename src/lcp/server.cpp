#include "config.hpp"
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

  // Instead of using threads 
  // Use non-blocking read with Edge triggered Epoll
  // Maintain a FIFO queue (using  Linked list)
  // Loop through the readyFds -> If not already present in the queue 
  // -> Add them -> Loop through the queue -> Process each client socket 
  // i.e., read 1023 bytes & if more data is present -> Add to the queue
  // Once the queue is processed -> Re-check epoll_wait & repeat.
  
  // TODO: 
  // Ensure the edge cases & processing time complexity & if this can lead to starvation of any kind 
  // Learn about level triggered & how does it compare to Edge triggered & what will be the flow using it
  // Compare throughly without favoritism
  
}
