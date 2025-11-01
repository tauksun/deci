// TODO: Build a stateful incremental parser, this has many flaws which will
// break, Writing better parser is must & priority

// sample messages
//
// *4\r\n$3\r\nSET\r\n$4\r\nName\r\n$5\r\nChiba\r\n:1\r\n
// *2\r\n$3\r\nGET\r\n$4\r\nName\r\n
// *2\r\n$4\r\nGGET\r\n$4\r\nName\r\n

#include "decoder.hpp"

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

DecodedMessage decoder(string &str) {
  DecodedMessage msg;

  int len = str.length();

  if (len < 2) {
    msg.error.partial = true;
    return msg;
  }

  if (str[0] != '*') {
    msg.error.invalid = true;
    return msg;
  }

  // Fetch number of elements in array, use it to decide if
  // message is partial
  int numberOfElements = stoi(&str[1]);
  int count = 0;
  int offset = 0;

  // Extract operation
  if (len < 6) {
    msg.error.partial = true;
    return msg;
  }

  if (str[4] != '$') {
    msg.error.invalid = true;
    return msg;
  }

  offset = 5;

  // Fetch operation length
  int oplength = extractLength(offset, str);

  if (len < offset + oplength) {
    msg.error.partial = true;
    return msg;
  }

  string op = str.substr(offset, oplength);
  msg.operation = op;
  // Increment element count
  count++;
  offset += oplength + 2;

  if (op == "GET" || op == "GGET" || op == "GDEL" || op == "EXISTS" ||
      op == "GEXISTS") {
    // Extract : key
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int keyLength = extractLength(offset, str);
    msg.key = str.substr(offset, keyLength);
    count++;
  } else if (op == "DEL") {
    // Extract : key, timestamp, sync
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int keyLength = extractLength(offset, str);
    msg.key = str.substr(offset, keyLength);
    count++;
    offset += keyLength + 2;

    // Timestamp
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int timestampLength = extractLength(offset, str);
    msg.timestamp = stol(str.substr(offset, timestampLength));
    count++;
    offset += timestampLength + 2;

    // Sync
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != ':') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    msg.flag.sync = stoi(&str[offset]);
    count++;
  } else if (op == "SET") {
    // Extract : key, value, timestamp, sync
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int keyLength = extractLength(offset, str);
    msg.key = str.substr(offset, keyLength);
    count++;
    offset += keyLength + 2;

    // Value
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int valueLength = extractLength(offset, str);
    msg.value = str.substr(offset, valueLength);
    count++;
    offset += valueLength + 2;

    // Timestamp
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int timestampLength = extractLength(offset, str);
    msg.timestamp = stol(str.substr(offset, timestampLength));
    count++;
    offset += timestampLength + 2;

    // Sync
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != ':') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    msg.flag.sync = stoi(&str[offset]);
    count++;

  } else if (op == "GSET") {
    // Extract : key, value

    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int keyLength = extractLength(offset, str);
    msg.key = str.substr(offset, keyLength);
    count++;
    offset += keyLength + 2;

    // Value
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int valueLength = extractLength(offset, str);
    msg.value = str.substr(offset, valueLength);
    count++;
    offset += valueLength + 2;
  } else if (op == "GHEALTH") {
    // Extract : timestamp
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int timestampLength = extractLength(offset, str);
    msg.timestamp = stol(str.substr(offset, timestampLength));
    count++;
    offset += timestampLength + 2;

  } else if (op == "GREGISTRATION") {
    // Extract : Group, LCP, Type

    // Group
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int groupLength = extractLength(offset, str);
    msg.reg.group = str.substr(offset, groupLength);
    count++;
    offset += groupLength + 2;

    // LCP
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int lcpLength = extractLength(offset, str);
    msg.reg.lcp = str.substr(offset, lcpLength);
    count++;
    offset += lcpLength + 2;

    // Type
    if (len < offset) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int typeLength = extractLength(offset, str);
    msg.reg.type = str.substr(offset, typeLength);
    count++;
    offset += typeLength + 2;
  }

  if (count < numberOfElements) {
    msg.error.partial = true;
    return msg;
  }
  return msg;
};
