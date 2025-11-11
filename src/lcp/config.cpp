#include "config.hpp"
#include "../common/configParser.hpp"
#include "../common/logger.hpp"

I_CONFIG lcp;
LCP_CONFIG configLCP;

void readConfig() {
  logger("Creating config for LCP");
  lcp.type = "lcp";
  lcp.lcp_config = &configLCP;
  createConfig(lcp);
  logger("Successfully created LCP config");
}
