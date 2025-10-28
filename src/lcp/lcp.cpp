#include "lcp.hpp"
#include "../common/logger.hpp"
#include "../deps/concurrentQueue.hpp"
#include "config.hpp"
#include "globalCacheOps.hpp"
#include "health.hpp"
#include "server.hpp"
#include "synchronizationOps.hpp"
#include <functional>
#include <sys/eventfd.h>
#include <thread>

int main() {
  initializeLogger();
  logger("Starting lcp");

  logger("Intializing listening server");

  // Event fds for triggering peer threads
  int globalCacheThreadEventFd = eventfd(0, EFD_NONBLOCK);
  int synchronizationEventFd = eventfd(0, EFD_NONBLOCK);

  // Concurrent queues
  moodycamel::ConcurrentQueue<GlobalCacheOpMessage> GlobalCacheOpsQueue;
  moodycamel::ConcurrentQueue<Operation> SynchronizationQueue;

  std::thread serverThread(server, configLCP::sock, ref(GlobalCacheOpsQueue),
                           ref(SynchronizationQueue));
  std::thread healthThread(health);
  std::thread GlobalCacheOpsThread(globalCacheOps, ref(GlobalCacheOpsQueue),
                                   globalCacheThreadEventFd);
  std::thread SynchronizationThread(
      cacheSynchronization, ref(SynchronizationQueue), synchronizationEventFd);

  // TODO: Learn more about this & the best practices around it
  serverThread.join();
  healthThread.join();
  GlobalCacheOpsThread.join();
  SynchronizationThread.join();

  // Ques :
  // Can you prevent the stale data by creating a flag, that when is true
  // returns the response only after the data ( key:value pair ) is synchronized
  // with all cache instances.
}
