#ifndef GCP_GROUP
#define GCP_GROUP

#include "../deps/concurrentQueue.hpp"
#include "gcp.hpp"

int group(int eventFd,
          moodycamel::ConcurrentQueue<GroupConcurrentSyncQueueMessage> &queue);

#endif
