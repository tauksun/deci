#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "config.hpp"
#include "connect.hpp"
#include <chrono>
#include <unistd.h>
void health() {

  // Establish a connection with GCP
  //  & update health every n seconds

  int connSockFd =
      establishConnection(configLCP::GCP_SERVER_IP, configLCP::GCP_SERVER_PORT);

  // Make fd non-blocking
  if (makeSocketNonBlocking(connSockFd)) {
    perror("health thread : fcntl connSockFd");
  }

  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();

  while (1) {
    logger("Updating health, ms : ", ms);
    write(connSockFd, &ms, sizeof(ms));
    sleep(configLCP::healthUpdateTime);
  }
}
