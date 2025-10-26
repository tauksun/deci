#ifndef LCP_CACHE_SYNCHRONIZATION
#define LCP_CACHE_SYNCHRONIZATION

#include "../deps/concurrentQueue.hpp"
#include "server.hpp"

void cacheSynchronization(
    moodycamel::ConcurrentQueue<Operation> &SynchronizationQueue);

#endif
