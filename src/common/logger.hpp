#ifndef COMMON_LOGGER
#define COMMON_LOGGER

#include <functional>
#include <sstream>
#include <string>

void logImpl(const std::string &s);
void logNoop(const std::string &);

extern std::function<void(const std::string &)>
    logFunc; // extern, defined in .cpp

void initializeLogger();

template <typename... Args> void logger(Args &&...args) {
  std::ostringstream oss;
  (oss << ... << args);
  logFunc(oss.str());
}

#endif
