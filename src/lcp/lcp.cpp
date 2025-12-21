#include "lcp.hpp"
#include "../common/logger.hpp"
#include "../common/randomId.hpp"
#include "../deps/concurrentQueue.hpp"
#include "config.hpp"
#include "globalCacheOps.hpp"
#include "registration.hpp"
#include "server.hpp"
#include "synchronizationOps.hpp"
#include "wal.hpp"
#include <functional>
#include <sys/eventfd.h>
#include <thread>

int main() {
  initializeLogger("/var/log/lcp/lcp.log");
  string lcpId = generateRandomId();
  logger("Starting lcp : ", lcpId);

  readConfig();

  logger("Intializing listening server");

  // Event fds for triggering peer threads
  int globalCacheThreadEventFd = eventfd(0, EFD_NONBLOCK);
  int synchronizationEventFd = eventfd(0, EFD_NONBLOCK);
  int walSyncEventFd = eventfd(0, EFD_NONBLOCK);

  // Concurrent queues
  moodycamel::ConcurrentQueue<GlobalCacheOpMessage> GlobalCacheOpsQueue;
  moodycamel::ConcurrentQueue<Operation> SynchronizationQueue;
  moodycamel::ConcurrentQueue<DecodedMessage> WalSyncQueue;

  std::thread serverThread(server, configLCP.sock.c_str(),
                           ref(GlobalCacheOpsQueue), ref(SynchronizationQueue),
                           globalCacheThreadEventFd, synchronizationEventFd,
                           ref(WalSyncQueue), walSyncEventFd);

  // Synchronously register with GCP
  lcpRegistration();

  std::thread GlobalCacheOpsThread(globalCacheOps, ref(GlobalCacheOpsQueue),
                                   globalCacheThreadEventFd, lcpId);
  std::thread SynchronizationThread(
      cacheSynchronization, ref(SynchronizationQueue), synchronizationEventFd,
      lcpId, ref(GlobalCacheOpsQueue), globalCacheThreadEventFd);
  std::thread WalSyncThread(walSync, ref(WalSyncQueue), walSyncEventFd, lcpId);
  WalSyncThread.detach();

  // TODO: Learn more about this & the best practices around it
  serverThread.join();
  GlobalCacheOpsThread.join();
  SynchronizationThread.join();

  // Ques :
  // Can you prevent the stale data by creating a flag, that when is true
  // returns the response only after the data ( key:value pair ) is synchronized
  // with all cache instances.
}
