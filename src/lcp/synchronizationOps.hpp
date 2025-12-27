#ifndef LCP_CACHE_SYNCHRONIZATION
#define LCP_CACHE_SYNCHRONIZATION

#include "../common/common.hpp"
#include "../deps/concurrentQueue.hpp"
#include "lcp.hpp"
#include <string>

void cacheSynchronization(
    moodycamel::ConcurrentQueue<Operation> &SynchronizationQueue,
    int synchronizationEventFd, string lcpId,
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue,
    int globalCacheThreadEventFd);

#endif
