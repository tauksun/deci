#ifndef LCP_SERVER
#define LCP_SERVER

#include "../common/messageParser.hpp"
#include <string>
using namespace std;

void server(const char *);
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

#endif
