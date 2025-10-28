#ifndef COMMON_OPERATE
#define COMMON_OPERATE

#include "common.hpp"
#include <unordered_map>
void operate(Operation &op, WriteSocketMessage *response,
             unordered_map<string, string> &cache);

#endif
