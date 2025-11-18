#include "logger.hpp"
#include "config.hpp"
#include <fstream>
#include <iostream>
#include <mutex>

static std::ofstream logFile;
static std::mutex logMutex;

bool initializeLogFile(const std::string &path) {
  logFile.open(path, std::ios::out | std::ios::app);
  return logFile.is_open();
}

void logImpl(const std::string &s) {
  std::lock_guard<std::mutex> guard(logMutex);

  std::cout << s << std::endl;

  if (logFile.is_open()) {
    logFile << s << std::endl;
    logFile.flush();
  }
}

void logNoop(const std::string &) { /* do nothing */ }

std::function<void(const std::string &)> logFunc;

void initializeLogger(const std::string filepath) {
  if (configCommon::logger) {
    initializeLogFile(filepath);
    logFunc = logImpl;
  } else {
    logFunc = logNoop;
  }
}
