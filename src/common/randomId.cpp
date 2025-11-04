#include "logger.hpp"
#include <climits>
#include <random>
#include <string>

std::string generateRandomId() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<unsigned long long> dist(0, ULLONG_MAX);

  std::string id = std::to_string(dist(gen));
  logger("Random Id : ", id);
  return id;
}
