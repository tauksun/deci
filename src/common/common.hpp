#ifndef COMMON
#define COMMON

#include "decoder.hpp"
#include <string>
using namespace std;

struct ReadSocketMessage {
  int fd;
  int readBytes = 0;
  string data = "";
};

struct Operation {
  int fd;
  DecodedMessage msg;
};

struct WriteSocketMessage {
  int fd;
  int writtenBytes = 0;
  string response = "";
};

struct WriteSocketSyncMessage {
  int fd;
  int writtenBytes = 0;
  string query = "";
};

#endif
