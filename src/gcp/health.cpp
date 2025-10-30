#include "../common/logger.hpp"

int epollFd;

void health() {
  logger("Health");
}
int addLCPConnectionForHealthMonitoring(int socketFd, std::string group,
                                        std::string lcp) {
  return 0;
}
