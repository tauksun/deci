#ifndef COMMON_RESPONSE_DECODER
#define COMMON_RESPONSE_DECODER

#include <string>
using namespace std;

struct ResponseDecodeError {
  bool partial = false;
  bool invalid = false;
};

struct DecodedResponse {
  string data;
  string dataType;
  ResponseDecodeError error;
};

DecodedResponse responseDecoder(std::string &response);

#endif
