#include "globalCacheOps.hpp"
#include <deque>

void globalCacheOps(
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue) {

  std::deque<int> connectionPool;

}
