#include "operate.hpp"
#include "encoder.hpp"

void operate(Operation &op, WriteSocketMessage &response,
             unordered_map<string, CacheValue> &cache) {

  if (op.msg.operation == "GET" || op.msg.operation == "GGET") {
    auto val = cache.find(op.msg.key);
    if (val != cache.end()) {
      response.response = encoder(&val->second.data, "string");
    } else {
      response.response = encoder(nullptr, "null");
    }
  } else if (op.msg.operation == "EXISTS" || op.msg.operation == "GEXISTS") {
    auto val = cache.find(op.msg.key);
    string res;
    if (val != cache.end()) {
      res = "1";
    } else {
      res = "0";
    }
    response.response = encoder(&res, "integer");
  } else if (op.msg.operation == "SET") {
    // Check if the value already exists & its timestamp
    string res;
    auto val = cache.find(op.msg.key);
    if (val != cache.end()) {
      // Only set value if the query timestamp is ahead than stored
      if (op.msg.timestamp > val->second.timestamp) {
        cache[op.msg.key].data = op.msg.value;
        cache[op.msg.key].timestamp = op.msg.timestamp;
        res = "1";
      } else {
        res = "0";
      }
    } else {
      cache[op.msg.key].data = op.msg.value;
      cache[op.msg.key].timestamp = op.msg.timestamp;
      res = "1";
    }

    response.response = encoder(&res, "integer");
  } else if (op.msg.operation == "DEL") {
    string res;
    auto val = cache.find(op.msg.key);
    if (val != cache.end()) {
      // Only delete value if the query timestamp is ahead than stored
      if (op.msg.timestamp > val->second.timestamp) {
        cache.erase(op.msg.key);
        res = "1";
      } else {
        res = "0";
      }
    } else {
      res = "0";
    }

    response.response = encoder(&res, "integer");
  } else if (op.msg.operation == "GSET") {
    string res;
    cache[op.msg.key].data = op.msg.value;
    res = "1";
    response.response = encoder(&res, "integer");
  } else if (op.msg.operation == "GDEL") {
    string res;
    auto val = cache.find(op.msg.key);
    if (val != cache.end()) {
      cache.erase(op.msg.key);
      res = "1";
    } else {
      res = "0";
    }
    response.response = encoder(&res, "integer");
  }
}
