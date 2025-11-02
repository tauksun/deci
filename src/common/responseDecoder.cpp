/**
 * Decodes :
 *      string ($)
 *      integer (:)
 *      null (_)
 *      error (-)
 * */

#include "responseDecoder.hpp"
#include <string>

int extractLength(int &offset, string &str) {
  string oplen = "";
  for (int i = offset; str[i] != '\r'; i++) {

    oplen += str[i];
    offset++;
  }

  // Increment offset to the pos after the limiters
  offset += 2;
  return stoi(oplen);
}

DecodedResponse responseDecoder(string &response) {
  DecodedResponse decodedResponse;

  if (response.length() < 2) {
    decodedResponse.error.partial = true;
    return decodedResponse;
  }

  char type = response[0];
  int offset = 1;

  // $3\r\nyup\r\n
  if (type == '$') {
    int strlen = extractLength(offset, response);

    if (response.length() < offset + strlen + 1) { // +1 for delimiter
      decodedResponse.error.partial = true;
      return decodedResponse;
    }
  }
  // :1234\r\n
  else if (type == ':') {
    int integerLength = extractLength(offset, response);
    if (response.length() < offset + integerLength + 1) { // +1 for delimiter
      decodedResponse.error.partial = true;
      return decodedResponse;
    }
  }
  // _\r\n
  else if (type == '_') {
    if (response.length() < 3) {
      decodedResponse.error.partial = true;
      return decodedResponse;
    }
  }
  // -Invalid Message\r\n
  else if (type == '-') {
    int len = response.length();
    while (offset < len && response[offset] != '\r') {
      offset++;
    }

    if (response[offset] != '\r') {
      decodedResponse.error.partial = true;
      return decodedResponse;
    }
  } else {
    decodedResponse.error.invalid = true;
    return decodedResponse;
  }

  return decodedResponse;
}
