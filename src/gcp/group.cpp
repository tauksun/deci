#include "group.hpp"
#include "../common/logger.hpp"
#include <deque>
#include <string>
#include <unordered_map>

void group(
    int epollFd,
    moodycamel::ConcurrentQueue<GroupConcurrentSyncQueueMessage> &queue) {

  logger("Group");
  std::unordered_map<std::string, std::deque<int> &> lcpQueueMap;

}
