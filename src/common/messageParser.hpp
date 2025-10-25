#ifndef COMMON_MSG_PARSER
#define COMMON_MSG_PARSER

#include <string>
using namespace std;

struct Flag {
  bool sync = false;
};
struct ParseError {
  bool partial = false;
  bool invalid = false;
};

struct ParsedMessage {
  string operation;
  string key;
  string value;
  Flag flag;
  ParseError error;
};

ParsedMessage msgParser(string &msg);

#endif
