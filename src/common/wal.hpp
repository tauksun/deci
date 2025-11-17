#ifndef COMMON_WAL
#define COMMON_WAL

#include <string>
std::string generateWalFileName(std::string &group);
int writeSync(std::string &response, int connSock);

#endif
