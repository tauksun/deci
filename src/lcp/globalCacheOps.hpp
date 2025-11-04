#ifndef LCP_GLOBAL_CACHE_OPS
#define LCP_GLOBAL_CACHE_OPS

#include "../deps/concurrentQueue.hpp"
#include "lcp.hpp"
#include <string>

void globalCacheOps(
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue,
    int globalCacheThreadEventFd, std::string lcpId);

#endif
