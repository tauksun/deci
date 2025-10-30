#ifndef GCP_HEALTH
#define GCP_HEALTH

#include <string>

void health();
int addLCPConnectionForHealthMonitoring(int socketFd, std::string group,
                                        std::string lcp);

#endif
