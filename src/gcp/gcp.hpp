#ifndef GCP
#define GCP

#include "../deps/concurrentQueue.hpp"
#include <string>

struct GroupConcurrentSyncQueueMessage {
  int fd;
  std::string lcp;
  std::string query;
  bool connectionRegistration = false;
};

struct GroupQueueEventFd {
  moodycamel::ConcurrentQueue<GroupConcurrentSyncQueueMessage> queue;
  int eventFd;
};

#endif
