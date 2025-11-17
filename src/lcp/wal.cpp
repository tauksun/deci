#include "../common/wal.hpp"
#include "../common/config.hpp"
#include "../common/decoder.hpp"
#include "../common/encoder.hpp"
#include "../common/logger.hpp"
#include "config.hpp"
#include "connect.hpp"
#include "registration.hpp"
#include "wal.hpp"
#include <chrono>
#include <pthread.h>
#include <unistd.h>

void walSync(moodycamel::ConcurrentQueue<DecodedMessage> &WalSyncQueue,
             int walSyncEventFd, std::string lcpId) {

  logger("WAL thread : started");
  // Connect with GCP
  int connSockFd = establishConnection(configLCP.GCP_SERVER_IP.c_str(),
                                       configLCP.GCP_SERVER_PORT);
  if (connSockFd == -1) {
    perror("WAL thread : Failed to establishConnection with GCP");
    pthread_exit(0);
  }

  // Synchronously register connection
  if (connectionRegistration(connSockFd, configCommon::WAL_SYNC_CONNECTION_TYPE,
                             lcpId) == -1) {
    logger("WAL thread : Failed to register connection with GCP");
    pthread_exit(0);
  }

  logger(
      "WAL thread : Successfully registered connection with GCP, connSockFd : ",
      connSockFd);

  auto syncStartTimestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  logger("WAL thread : Wal Sync stopping timestamp : ", syncStartTimestamp);

  vector<QueryArrayElement> walCommunication;

  // Operation
  QueryArrayElement op;
  op.value = "GWALSYNC";
  op.type = "string";

  // Start
  QueryArrayElement start;
  start.value = "START";
  start.type = "string";

  // Continue
  QueryArrayElement continueEle;
  continueEle.value = "CONTINUE";
  continueEle.type = "string";

  // Stop
  QueryArrayElement stop;
  stop.value = "STOP";
  stop.type = "string";

  // Start WAL communication with GCP with START message
  walCommunication.push_back(op);
  walCommunication.push_back(start);

  string startMessage = encoder(walCommunication);
  logger("WAL thread : Sending START to GCP, connSockFd : ", connSockFd,
         " startMessage : ", startMessage);
  if (writeSync(startMessage, connSockFd) == -1) {
    logger("WAL thread : Error while writing to connSockFd : ", connSockFd);
    close(connSockFd);
    pthread_exit(0);
  }

  // Construct continue message once for loop
  walCommunication.pop_back();
  walCommunication.push_back(continueEle);
  string continueMessage = encoder(walCommunication);
  logger("WAL thread : continueMessage : ", continueMessage);

  // Read the WAL & push to ConcurrentQueue
  long seeker = 0;
  string walLog;

  while (true) {
    int bufferSize = 1023;
    char buf[bufferSize];
    int readBytes = read(connSockFd, buf, bufferSize);

    if (readBytes <= 0) {
      logger("WAL thread : Error while reading from connSockFd : ", connSockFd,
             " readBytes : ", readBytes);
      close(connSockFd);
      pthread_exit(0);
      break;
    }

    walLog.append(buf, readBytes);
    seeker += readBytes;

    // Decode
    logger("WAL thread : Decoding walLog");
    bool isConcurrentMessageQueued = false;
    while (true) {
      logger("WAL thread : In while for decoding");
      DecodedMessage decodedWalMessage = decoder(walLog, true);

      if (decodedWalMessage.error.invalid) {
        logger("WAL thread : Invalid Wal Message, walLog : ", walLog);
        close(connSockFd);
        pthread_exit(0);
      }

      if (decodedWalMessage.error.partial) {
        logger("WAL thread : Partially decoded message, breaking loop");
        break;
      }

      logger(
          "WAL thread : Decoded message : op : ", decodedWalMessage.operation,
          " messageLength : ", decodedWalMessage.messageLength);
      if (decodedWalMessage.key == "EOF") {
        logger("WAL thread : Reached EOF");
        close(connSockFd);
        pthread_exit(0);
      }

      walLog = walLog.substr(decodedWalMessage.messageLength);

      if (decodedWalMessage.timestamp > syncStartTimestamp) {
        logger("WAL thread : Stopping WAL sync as timestamp reached. "
               "syncStartTimestamp : ",
               syncStartTimestamp,
               " wal message timestamp : ", decodedWalMessage.timestamp);

        walCommunication.push_back(stop);
        string stopMessage = encoder(walCommunication);

        logger("WAL thread : Writing STOP to GCP, stopMessage : ", stopMessage);
        writeSync(stopMessage, connSockFd);
        close(connSockFd);
        pthread_exit(0);
      }

      logger("WAL thread : Pushing decoded message to ConcurrentQueue");
      WalSyncQueue.enqueue(decodedWalMessage);
      isConcurrentMessageQueued = true;
    }

    // Trigger eventFd
    logger("WAL thread : Checking if to trigger eventFd, "
           "isConcurrentMessageQueued : ",
           isConcurrentMessageQueued);
    if (isConcurrentMessageQueued) {
      logger("WAL thread : Triggering eventFd");
      logger("WAL thread : triggerEventfd for walSyncEventFd");
      uint64_t counter = 1;
      write(walSyncEventFd, &counter, sizeof(counter));
      logger("WAL thread : walSyncEventFd counter : ", counter);
    }

    // Write continue to GCP
    logger("WAL thread : Writing continue message to GCP");
    if (writeSync(continueMessage, connSockFd) == -1) {
      logger("WAL thread : Error while writing continue message to GCP");
      close(connSockFd);
      pthread_exit(0);
    }
  }
}
