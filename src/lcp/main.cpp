#include "main.hpp"
#include "../common/logger.hpp"
#include "../deps/concurrentQueue.hpp"
#include "config.hpp"
#include "globalCacheOps.hpp"
#include "health.hpp"
#include "server.hpp"
#include "synchronizationOps.hpp"
#include <functional>
#include <thread>

int main() {
  initializeLogger();
  logger("Starting lcp");

  logger("Intializing listening server");

  // Concurrent queues
  moodycamel::ConcurrentQueue<GlobalCacheOpMessage> GlobalCacheOpsQueue;
  moodycamel::ConcurrentQueue<Operation> SynchronizationQueue;

  std::thread serverThread(server, configLCP::sock, ref(GlobalCacheOpsQueue),
                           ref(SynchronizationQueue));
  std::thread healthThread(health);
  std::thread GlobalCacheOpsThread(globalCacheOps, ref(GlobalCacheOpsQueue));
  std::thread SynchronizationThread(cacheSynchronization,
                                    ref(SynchronizationQueue));
  serverThread.join();
  healthThread.join();
  GlobalCacheOpsThread.join();
  SynchronizationThread.join();
}
