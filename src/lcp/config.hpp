#ifndef LCP_CONSTANTS
#define LCP_CONSTANTS

namespace configLCP {
constexpr int MAXCONNECTIONS = 100;
constexpr int SOCKET_REUSE = 1;
constexpr const char *sock = "/tmp/lcp.sock";
constexpr int MAX_READ_BYTES = 1023;
constexpr int MAX_WRITE_BYTES = 1023;
constexpr int healthUpdateTime = 3; // Seconds
constexpr unsigned long MAX_SYNC_MESSAGES = 100;
constexpr int MAX_SYNC_CONNECTIONS = 100;
constexpr const char *GCP_SERVER_IP = "127.0.0.1";
constexpr int GCP_SERVER_PORT = 7480;
constexpr int MAX_GCP_CONNECTIONS = 100;

} // namespace configLCP

#endif
