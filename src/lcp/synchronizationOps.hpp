#ifndef LCP_CACHE_SYNCHRONIZATION
#define LCP_CACHE_SYNCHRONIZATION

#include "../common/common.hpp"
#include "../deps/concurrentQueue.hpp"

void cacheSynchronization(
    moodycamel::ConcurrentQueue<Operation> &SynchronizationQueue,
    int synchronizationEventFd);

#endif
