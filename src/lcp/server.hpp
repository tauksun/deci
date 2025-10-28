#ifndef LCP_SERVER
#define LCP_SERVER

#include "../common/common.hpp"
#include "../deps/concurrentQueue.hpp"
#include "lcp.hpp"
using namespace std;

void server(
    const char *,
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue,
    moodycamel::ConcurrentQueue<Operation> &SynchronizationQueue,
    int globalCacheThreadEventFd, int synchronizationEventFd);

#endif
