#ifndef COMMON_CONFIG_PARSER
#define COMMON_CONFIG_PARSER

#include <string>
using namespace std;

struct LCP_CONFIG {
  string GROUP = "deci";
  int MAX_CONNECTIONS = 100;
  int SOCKET_REUSE = 1;
  string sock = "/tmp/lcp.sock";
  int MAX_READ_BYTES = 1023;
  int MAX_WRITE_BYTES = 1023;
  int healthUpdateTime = 3; // Seconds
  unsigned long MAX_SYNC_MESSAGES = 100;
  int MAX_SYNC_CONNECTIONS = 2; // Receiving sync operations from GCP
  string GCP_SERVER_IP = "127.0.0.1";
  int GCP_SERVER_PORT = 7480;
  unsigned long MAX_GCP_CONNECTIONS = 2;
};

struct GCP_CONFIG {
  int MAX_CONNECTIONS = 1000;
  int MAX_READ_BYTES = 1023;
  int MAX_WRITE_BYTES = 1023;
  int SERVER_PORT = 7480;
  int WAL_EPOLL_CONNECTIONS = 1;
};

struct I_CONFIG {
  LCP_CONFIG *lcp_config;
  GCP_CONFIG *gcp_config;
  string type;
};

void createConfig(I_CONFIG &);

#endif
