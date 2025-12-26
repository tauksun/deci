#ifndef GCP
#define GCP

#include "../deps/concurrentQueue.hpp"
#include <string>

struct GroupConcurrentSyncQueueMessage {
  int fd;
  std::string lcp;
  std::string query;
  bool connectionRegistration = false;
  int pingMessage = 0; // Stores the required connection by LCP
};

struct GroupQueueEventFd {
  moodycamel::ConcurrentQueue<GroupConcurrentSyncQueueMessage> queue;
  int eventFd;
};

#endif
