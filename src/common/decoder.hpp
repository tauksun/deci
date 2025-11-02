#ifndef COMMON_DECODER
#define COMMON_DECODER

#include "common.hpp"
#include <string>
using namespace std;

DecodedMessage decoder(string &);

int extractLength(int &offset, std::string &str);

#endif
