#include "logger.hpp"
#include <fcntl.h>
#include <unistd.h>

int makeSocketNonBlocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1) {
    return 1;
  }
  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    return 1;
  }
  return 0;
}

int makeSocketBlocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1) {
    return 1; // error
  }
  flags &= ~O_NONBLOCK; // clear the non-blocking flag
  if (fcntl(fd, F_SETFL, flags) == -1) {
    return 1; // error
  }
  return 0; // success
}

bool drainSocketSync(int fd) {
  int bufferSize = 10;
  char buf[bufferSize];
  int readBytes = read(fd, buf, bufferSize);

  if (readBytes == 0) {
    return false;
  } else if (readBytes < 0) {
    logger("drainSocketSync : readBytes < 0, for fd : ", fd);
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      logger("drainSocketSync : Fully drained fd : ", fd);
      return true;
    } else {
      logger("drainSocketSync : Error while reading, fd : ", fd);
      perror("drainSocketSync : Error while reading from socket");
      return false;
    }
  } else if (readBytes > 0) {
    logger("drainSocketSync : Recursively draining, as readBytes > 0, "
           "readBytes : ",
           readBytes);
    return drainSocketSync(fd);
  }

  return false;
}
