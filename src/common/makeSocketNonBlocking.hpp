#ifndef COMMON_MSNB
#define COMMON_MSNB

int makeSocketNonBlocking(int fd);
int makeSocketBlocking(int fd);
bool drainSocketSync(int fd);

#endif
