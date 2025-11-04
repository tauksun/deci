#ifndef COMMON_ENCODER
#define COMMON_ENCODER

#include <string>
#include <vector>
using namespace std;

struct QueryArrayElement {
  string type;
  string value;
};

string encoder(string *str, string type);
string encoder(vector<QueryArrayElement> &params);

#endif
