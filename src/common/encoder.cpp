#include "encoder.hpp"

string delimiter = "\r\n";
string encoder(string *str, string type) {
  string msg;

  if (type == "null") {
    msg = "_" + delimiter;
  } else if (type == "string") {
    int len = (*str).length();
    msg = "$" + to_string(len) + delimiter + *str + delimiter;
  } else if (type == "error") {
    msg = "-" + delimiter + *str + delimiter;
  } else if (type == "integer") {
    msg = ":" + *str + delimiter;
  }

  return msg;
}
