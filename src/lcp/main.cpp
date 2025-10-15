#include "../common/logger.hpp"
#include "server.hpp"
int main() {
  initializeLogger();
  logger("Starting lcp");
  logger("Intializing listening server");
  server("/tmp/lcp.sock");
  //
}
