#ifndef LCP_GLOBAL_CACHE_OPS
#define LCP_GLOBAL_CACHE_OPS

#include "../deps/concurrentQueue.hpp"
#include "main.hpp"

void globalCacheOps(
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue);

#endif
