#include "configParser.hpp"
#include "logger.hpp"
#include <fstream>
#include <functional>
#include <string>
#include <unordered_map>

using namespace std;

// TODO:
// Read lcp.config / gcp.config from the same directory as the binary for now
// Change this to /usr/local/etc when working on package installer

struct KeyValue {
  std::string key;
  std::string value;
};

void trimmer(std::string &str) {
  int start = 0;
  int end = int(str.length()) - 1;

  // trim from start
  while (start <= end && std::isspace(static_cast<unsigned char>(str[start]))) {
    ++start;
  }

  // trim from end
  while (end >= start && std::isspace(static_cast<unsigned char>(str[end]))) {
    --end;
  }

  str = (start > end) ? "" : str.substr(start, end - start + 1);
}

int reader(std::ifstream &f, KeyValue &kv) {
  std::string line;
  if (!getline(f, line)) {
    return -1;
  };

  if (line.empty()) {
    return 0;
  }

  // Check for comment
  int pos = line.find("#");
  line = line.substr(0, pos);
  if (line.empty()) {
    return 0;
  }

  // Extract key:value
  int limiter = line.find("=");
  std::string key = line.substr(0, limiter);
  std::string value = line.substr(limiter + 1, line.length());
  trimmer(key);
  trimmer(value);

  kv.key = key;
  kv.value = value;
  return 1;
}

void createConfig(I_CONFIG &configuration) {
  using namespace std;

  unordered_map<string, function<void(const string &)>> lcp_setters = {
      {"GROUP",
       [&](const string &val) { (*configuration.lcp_config).GROUP = val; }},
      {"MAX_CONNECTIONS",
       [&](const string &val) {
         (*configuration.lcp_config).MAX_CONNECTIONS = stoi(val);
       }},
      {"SOCKET_REUSE",
       [&](const string &val) {
         (*configuration.lcp_config).SOCKET_REUSE = stoi(val);
       }},
      {"sock",
       [&](const string &val) { (*configuration.lcp_config).sock = val; }},
      {"MAX_READ_BYTES",
       [&](const string &val) {
         (*configuration.lcp_config).MAX_READ_BYTES = stoi(val);
       }},
      {"MAX_WRITE_BYTES",
       [&](const string &val) {
         (*configuration.lcp_config).MAX_WRITE_BYTES = stoi(val);
       }},
      {"healthUpdateTime",
       [&](const string &val) {
         (*configuration.lcp_config).healthUpdateTime = stoi(val);
       }},
      {"MAX_SYNC_MESSAGES",
       [&](const string &val) {
         (*configuration.lcp_config).MAX_SYNC_MESSAGES = stoi(val);
       }},
      {"MAX_SYNC_CONNECTIONS",
       [&](const string &val) {
         (*configuration.lcp_config).MAX_SYNC_CONNECTIONS = stoi(val);
       }},
      {"GCP_SERVER_PORT",
       [&](const string &val) {
         (*configuration.lcp_config).GCP_SERVER_PORT = stoi(val);
       }},
      {"MAX_GCP_CONNECTIONS",
       [&](const string &val) {
         (*configuration.lcp_config).MAX_GCP_CONNECTIONS = stoi(val);
       }},
      {"GCP_SERVER_IP",
       [&](const string &val) {
         (*configuration.lcp_config).GCP_SERVER_IP = val;
       }},
  };

  unordered_map<string, function<void(const string &)>> gcp_setters = {
      {"MAX_CONNECTIONS",
       [&](const string &val) {
         (*configuration.gcp_config).MAX_CONNECTIONS = stoi(val);
       }},
      {"MAX_READ_BYTES",
       [&](const string &val) {
         (*configuration.gcp_config).MAX_READ_BYTES = stoi(val);
       }},
      {"MAX_WRITE_BYTES",
       [&](const string &val) {
         (*configuration.gcp_config).MAX_WRITE_BYTES = stoi(val);
       }},
      {"SERVER_PORT",
       [&](const string &val) {
         (*configuration.gcp_config).SERVER_PORT = stoi(val);
       }},
  };

  string file;
  unordered_map<string, function<void(const string &)>> *setters;

  if (configuration.type == "lcp") {
    file = "lcp.conf";
    setters = &lcp_setters;
  } else if (configuration.type == "gcp") {
    file = "gcp.conf";
    setters = &gcp_setters;
  }

  ifstream f(file);
  if (!f.is_open()) {
    logger("Unable to load LCP config");
    exit(EXIT_FAILURE);
  }

  // Map from config key to lambda that sets corresponding member

  while (true) {
    KeyValue kv;
    int read = reader(f, kv);
    if (read == -1)
      break;
    if (read == 0)
      continue;

    auto it = (*setters).find(kv.key);
    if (it != (*setters).end()) {
      try {
        it->second(kv.value);
      } catch (...) {
        logger("Error parsing value for key: ", kv.key);
      }
    } else {
      logger("Unknown config key: ", kv.key);
    }
  }
}
