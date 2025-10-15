#include "../common/logger.hpp"
#include "server.hpp"
int main() {
  initializeLogger();
  log("Starting lcp");
  log("Intializing listening server");
  server("/tmp/lcp.sock");
  //
}
