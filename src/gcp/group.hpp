#ifndef GCP_GROUP
#define GCP_GROUP

#include "../deps/concurrentQueue.hpp"
#include "gcp.hpp"
#include <string>

int group(int eventFd,
          moodycamel::ConcurrentQueue<GroupConcurrentSyncQueueMessage> &queue,
          std::string groupName);

#endif
