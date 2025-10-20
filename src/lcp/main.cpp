#include "../common/logger.hpp"
#include "config.hpp"
#include "server.hpp"
#include <thread>

int main() {
  initializeLogger();
  logger("Starting lcp");

  logger("Intializing listening server");
  std::thread serverThread(server, configLCP::sock);
}
