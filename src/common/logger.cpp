#include "logger.hpp"
#include "config.hpp"
#include <iostream>

void logImpl(const std::string &s) { std::cout << s << std::endl; }
void logNoop(const std::string &) { /* do nothing */ }

std::function<void(const std::string &)> logFunc;

void initializeLogger() {
  if (configCommon::logger)
    logFunc = logImpl;
  else
    logFunc = logNoop;
}
