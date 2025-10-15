#include "config.hpp"
#include <functional>
#include <iostream>
#include <string>

using namespace std;

void logImpl(const string &s) { cout << s << endl; }

void logNoop(const string &) {
  // do nothing
}

// Function pointer
function<void(const string &)> logFunc;

void initializeLogger() {
  if (configCommon::logger)
    logFunc = logImpl;
  else
    logFunc = logNoop;
}

void logger(const string &s) { logFunc(s); }
