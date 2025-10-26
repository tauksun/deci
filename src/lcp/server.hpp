#ifndef LCP_SERVER
#define LCP_SERVER

#include "../common/messageParser.hpp"
#include "../deps/concurrentQueue.hpp"
#include "main.hpp"
#include <string>
using namespace std;

struct ReadSocketMessage {
  int fd;
  int readBytes = 0;
  string data = "";
};

struct Operation {
  int fd;
  ParsedMessage msg;
};

struct WriteSocketMessage {
  int fd;
  int writtenBytes = 0;
  string response = "";
};

void server(
    const char *,
    moodycamel::ConcurrentQueue<GlobalCacheOpMessage> &GlobalCacheOpsQueue,
    moodycamel::ConcurrentQueue<Operation> &SynchronizationQueue);

#endif
