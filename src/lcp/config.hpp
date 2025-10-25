#ifndef LCP_CONSTANTS
#define LCP_CONSTANTS

namespace configLCP {
constexpr int MAXCONNECTIONS = 100;
constexpr int SOCKET_REUSE = 1;
constexpr const char *sock = "/tmp/lcp.sock";
constexpr long MAX_READ_BYTES = 1023;
constexpr long MAX_WRITE_BYTES = 1023;
} // namespace configLCP

#endif
