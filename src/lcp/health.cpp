#include "../common/logger.hpp"
#include "config.hpp"
#include <unistd.h>
void health() {
  while (1) {
    logger("Updating health");
    sleep(configLCP::healthUpdateTime);
  }
}
