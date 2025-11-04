#ifndef LCP_REGISTRATION
#define LCP_REGISTRATION

#include <string>

int lcpRegistration();

int connectionRegistration(int connSockFd, std::string type, std::string lcpId);

#endif
