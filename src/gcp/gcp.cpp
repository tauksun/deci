#include "gcp.hpp"
#include "../common/logger.hpp"
#include "health.hpp"
#include "server.hpp"
#include <thread>
#include <unordered_map>

int main() {
  initializeLogger();
  logger("Starting GCP");

  std::unordered_map<std::string, GroupQueueEventFd> groups;

  std::thread serverThread(server, ref(groups));
  std::thread healthThread(health);

  serverThread.join();
  healthThread.join();
}
