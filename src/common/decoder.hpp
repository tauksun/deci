#ifndef COMMON_DECODER
#define COMMON_DECODER

#include <string>
using namespace std;

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

DecodedMessage decoder(string &);

#endif
