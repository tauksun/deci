#include "operate.hpp"
#include "logger.hpp"

void operate(Operation &op, WriteSocketMessage *response,
             unordered_map<string, string> &cache) {
  if (op.msg.operation == "SET") {
    logger("Key : ", op.msg.key, " Value : ", op.msg.value,
           " Sync : ", op.msg.flag.sync);
    cache[op.msg.key] = op.msg.value;
    if (response != nullptr) {
      response->response = ":1\r\n";
    }

  } else if (op.msg.operation == "GET") {
    logger("Key : ", op.msg.key);
    auto val = cache.find(op.msg.key);
    if (val != cache.end()) {
      // // Convert this using protocol
      // // Convert this using protocol
      // // Convert this using protocol
      if (response != nullptr) {
        response->response = val->second;
      }
    } else {
      if (response != nullptr) {
        response->response =
            "_\r\n"; // Null // TODO: as per protocol using a function
      }
    }
  } else if (op.msg.operation == "DEL") {
    logger("Key : ", op.msg.key);
    cache.erase(op.msg.key);
    if (response != nullptr) {
      response->response = ":1\r\n";
    } // TODO: as per protocol using a function
  } else if (op.msg.operation == "EXISTS") {
    logger("Key : ", op.msg.key);
    auto val = cache.find(op.msg.key);
    if (val != cache.end()) {
      if (response != nullptr) {
        response->response = ":1\r\n";
      }
    } else {
      if (response != nullptr) {
        response->response = ":0\r\n";
      }
    }
  }
}
