#include "wal.hpp"
#include "logger.hpp"
#include <string>
#include <unistd.h>

std::string generateWalFileName(std::string &group) {
  std::string location = "/var/lib/gcp/";
  return location + group + ".wal";
}

int writeSync(std::string &response, int connSock) {
  int writtenBytes = 0;
  int responseLength = response.length();
  int writeResponse = 0;

  while (writtenBytes < responseLength) {
    int written = write(connSock, response.c_str(), response.length());

    if (written <= 0) {
      logger("WAL thread : Error while writing to connSock : ", connSock,
             " writtenBytes : ", writtenBytes);
      writeResponse = -1;
      break;
    }

    response = response.substr(written);
    writtenBytes += written;
  }

  return writeResponse;
}
