// TODO: Build a stateful incremental parser, this has many flaws which will
// break, Writing better parser is must & priority

// sample messages
//
// *2\r\n$3\r\nGET\r\n$4\r\nName\r\n
// *2\r\n$4\r\nGGET\r\n$4\r\nName\r\n

#include "decoder.hpp"
#include "logger.hpp"

int extractLength(int &offset, string &str) {
  string oplen = "";
  int len = str.length();
  for (int i = offset; str[i] != '\r'; i++) {
    if (len < offset + 1) {
      return -1;
    }
    oplen += str[i];
    offset++;
  }

  // Increment offset to the pos after the limiters
  offset += 2;
  return stoi(oplen);
}

DecodedMessage decoder(string &str, bool extractLen) {
  logger("DECODER : str : ", str);

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
  // TODO: Extract this dynamically in incremental parser implementation
  // As of now, it assumes single digit elements ( which works for now )
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

  if (oplength == -1 || len < offset + oplength + 1) {
    msg.error.partial = true;
    return msg;
  }

  string op = str.substr(offset, oplength);
  msg.operation = op;
  // Increment element count
  count++;
  offset += oplength + 2;

  logger("DECODER : op : ", op);
  if (op == "GET" || op == "GGET" || op == "GDEL" || op == "EXISTS" ||
      op == "GEXISTS") {
    // Extract : key
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int keyLength = extractLength(offset, str);
    if (keyLength == -1 || len < offset + keyLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.key = str.substr(offset, keyLength);
    count++;
  } else if (op == "DEL") {
    // Extract : key, timestamp, sync
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int keyLength = extractLength(offset, str);
    if (keyLength == -1 || len < offset + keyLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.key = str.substr(offset, keyLength);
    count++;
    offset += keyLength + 2;

    // Timestamp
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int timestampLength = extractLength(offset, str);
    if (timestampLength == -1 || len < offset + timestampLength + 1) {
      msg.error.partial = true;
      return msg;
    }

    msg.timestamp = stol(str.substr(offset, timestampLength));
    count++;
    offset += timestampLength + 2;

    // Sync
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != ':') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.flag.sync = stoi(&str[offset]);
    count++;

    // Used during applying WAL
    if (extractLen) {
      if (len < offset + 3) {
        msg.error.partial = true;
        return msg;
      }
      msg.messageLength = offset + 3;
    }

  } else if (op == "SET") {
    // Extract : key, value, timestamp, sync
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int keyLength = extractLength(offset, str);
    if (keyLength == -1 || len < offset + keyLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.key = str.substr(offset, keyLength);
    count++;
    offset += keyLength + 2;

    // Value
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int valueLength = extractLength(offset, str);
    if (valueLength == -1 || len < offset + valueLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.value = str.substr(offset, valueLength);
    count++;
    offset += valueLength + 2;

    // Timestamp
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int timestampLength = extractLength(offset, str);
    if (timestampLength == -1 || len < offset + timestampLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.timestamp = stol(str.substr(offset, timestampLength));
    count++;
    offset += timestampLength + 2;

    // Sync
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != ':') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.flag.sync = stoi(&str[offset]);
    count++;

    // Used during applying WAL
    if (extractLen) {
      if (len < offset + 3) {
        msg.error.partial = true;
        return msg;
      }
      msg.messageLength = offset + 3;
    }
  } else if (op == "GSET") {
    // Extract : key, value

    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int keyLength = extractLength(offset, str);
    if (keyLength == -1 || len < offset + keyLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.key = str.substr(offset, keyLength);
    count++;
    offset += keyLength + 2;

    // Value
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int valueLength = extractLength(offset, str);
    if (valueLength == -1 || len < offset + valueLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.value = str.substr(offset, valueLength);
    count++;
    offset += valueLength + 2;
  } else if (op == "GHEALTH") {
    // Extract : timestamp
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int timestampLength = extractLength(offset, str);
    if (timestampLength == -1 || len < offset + timestampLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.timestamp = stol(str.substr(offset, timestampLength));
    count++;
    offset += timestampLength + 2;

  } else if (op == "GREGISTRATION_CONNECTION") {
    // Extract : Group, LCP, Type

    // Group
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int groupLength = extractLength(offset, str);
    if (groupLength == -1 || len < offset + groupLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.reg.group = str.substr(offset, groupLength);
    count++;
    offset += groupLength + 2;

    // LCP
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int lcpLength = extractLength(offset, str);
    if (lcpLength == -1 || len < offset + lcpLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.reg.lcp = str.substr(offset, lcpLength);
    count++;
    offset += lcpLength + 2;

    // Type
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int typeLength = extractLength(offset, str);
    if (typeLength == -1 || len < offset + typeLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.reg.type = str.substr(offset, typeLength);
    count++;
    offset += typeLength + 2;
  } else if (op == "GREGISTRATION_LCP") {
    // Extract : Group

    // Group
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int groupLength = extractLength(offset, str);
    if (groupLength == -1 || len < offset + groupLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.reg.group = str.substr(offset, groupLength);
    count++;
    offset += groupLength + 2;
  } else if (op == "GWALSYNC") {
    // Extract : Wal sync operation as key
    // Wal sync operation
    if (len < offset + 1) {
      msg.error.partial = true;
      return msg;
    }
    if (str[offset] != '$') {
      msg.error.invalid = true;
      return msg;
    }
    offset++;

    int opLength = extractLength(offset, str);
    if (opLength == -1 || len < offset + opLength + 1) {
      msg.error.partial = true;
      return msg;
    }
    msg.key = str.substr(offset, opLength);
    count++;
    offset += opLength + 2;

    logger("DECODER : op : ", op, " key : ", msg.key);
    if (msg.key == "RESUME") {
      // Extract seek
      if (len < offset + 1) {
        msg.error.partial = true;
        return msg;
      }
      if (str[offset] != '$') {
        msg.error.invalid = true;
        return msg;
      }
      offset++;

      int seekerLength = extractLength(offset, str);
      if (seekerLength == -1 || len < offset + seekerLength + 1) {
        msg.error.partial = true;
        return msg;
      }
      msg.value = stol(str.substr(offset, seekerLength));
      count++;
      offset += seekerLength + 2;
    }

  } else {
    msg.error.invalid = true;
    return msg;
  }

  if (count < numberOfElements) {
    msg.error.partial = true;
    return msg;
  }
  return msg;
};
