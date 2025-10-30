#ifndef GCP_GROUP
#define GCP_GROUP

#include "../deps/concurrentQueue.hpp"
#include "gcp.hpp"

void group(int epollFd,
           moodycamel::ConcurrentQueue<GroupConcurrentSyncQueueMessage> &queue);

#endif
