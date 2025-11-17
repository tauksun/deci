#ifndef LCP_WAL
#define LCP_WAL

#include "../deps/concurrentQueue.hpp"
#include <string>
#include "../common/common.hpp"

void walSync(moodycamel::ConcurrentQueue<DecodedMessage> &WalSyncQueue,
             int walSyncEventFd, std::string lcpId);

#endif
