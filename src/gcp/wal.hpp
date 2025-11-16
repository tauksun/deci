#ifndef GCP_WAL
#define GCP_WAL

#include "../deps/concurrentQueue.hpp"
#include <cstdint>
#include <fstream>
#include <string>

void walWriter(std::fstream &f, std::string groupName,
               moodycamel::ConcurrentQueue<std::string> &walQueue, int eventFd);
void walReader(std::string &buf, int64_t seeker);

#endif
