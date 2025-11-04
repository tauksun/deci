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
    msg = "-" + *str + delimiter;
  } else if (type == "integer") {
    msg = ":" + *str + delimiter;
  }

  return msg;
}

string encoder(vector<QueryArrayElement> &params) {
  string query;

  int arraySize = params.size();
  query += "*" + to_string(arraySize) + delimiter;
  for (int i = 0; i < arraySize; i++) {
    query += encoder(&params[i].value, params[i].type);
  }

  return query;
}
