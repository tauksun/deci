#ifndef LCP_SERVER
#define LCP_SERVER

#include "gcp.hpp"
#include <string>
#include <unordered_map>

struct FdGroupLCP {
  std::string group;
  std::string lcp;
};

void server(std::unordered_map<std::string, GroupQueueEventFd> &groups);

#endif
