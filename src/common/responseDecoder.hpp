#ifndef COMMON_DECODER
#define COMMON_DECODER

#include <string>
using namespace std;

struct ResponseDecodeError {
  bool partial = false;
  bool invalid = false;
};

struct DecodedResponse {
  ResponseDecodeError error;
};

DecodedResponse responseDecoder(std::string &response);

#endif
