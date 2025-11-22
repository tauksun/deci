/**
 * Decodes :
 *      string ($)
 *      integer (:)
 *      null (_)
 *      error (-)
 * */

#include "responseDecoder.hpp"
#include "decoder.hpp"
#include <string>

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

    if (strlen == -1 ||
        response.length() < offset + strlen + 1) { // +1 for delimiter
      decodedResponse.error.partial = true;
      return decodedResponse;
    }
  }
  // :1234\r\n
  else if (type == ':') {
    int len = extractLength(offset, response);
    if (len == -1 || response.length() < offset) {
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
