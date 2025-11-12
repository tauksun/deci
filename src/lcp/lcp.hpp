#ifndef LCP_MAIN
#define LCP_MAIN

#include <string>

struct GlobalCacheOpMessage {
  int fd = 0;
  std::string op = "";
  bool partial = false;
  int connSock = 0;
};

#endif
