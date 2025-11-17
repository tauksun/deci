#include <fcntl.h>

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
