#include "config.hpp"
#include "../common/configParser.hpp"
#include "../common/logger.hpp"

I_CONFIG gcp;
GCP_CONFIG configGCP;

void readConfig() {
  logger("Creating config for GCP");
  gcp.type = "gcp";
  gcp.gcp_config = &configGCP;
  createConfig(gcp);
  logger("Successfully created GCP config");
}
