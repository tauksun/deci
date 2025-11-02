#ifndef COMMON
#define COMMON

#include <string>
using namespace std;

struct ReadSocketMessage {
  int fd;
  int readBytes = 0;
  string data = "";
};

struct Flag {
  bool sync = false;
};

struct DecodeError {
  bool partial = false;
  bool invalid = false;
};

struct Registration {
  string group = "";
  string lcp = "";
  string type = "";
};

struct DecodedMessage {
  string operation = "";
  string key = "";
  string value = "";
  int64_t timestamp = 0;
  Registration reg;
  Flag flag;
  DecodeError error;
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

struct CacheValue {
  string data;
  int64_t timestamp;
};

#endif
