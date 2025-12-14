#ifndef LCP_REGISTRATION
#define LCP_REGISTRATION

#include "../common/encoder.hpp"
#include <string>

int lcpRegistration();

int connectionRegistration(int connSockFd, std::string type, std::string lcpId);

vector<QueryArrayElement> connRegisterationMessage(std::string type,
                                                   std::string lcpId);

#endif
