#include "registration.hpp"
#include "../common/encoder.hpp"
#include "../common/logger.hpp"
#include "../common/responseDecoder.hpp"
#include "config.hpp"
#include "connect.hpp"
#include <cstdio>
#include <unistd.h>
#include <vector>

int lcpRegistration() {

  logger("Staring LCP Registration...");

  int regSock = establishConnection(configLCP.GCP_SERVER_IP.c_str(),
                                    configLCP.GCP_SERVER_PORT);
  if (regSock == -1) {
    perror("Cannot establish connection with GCP for registration");
    exit(EXIT_FAILURE);
  }

  logger("LCP Registration connection established, regSock: ", regSock);
  logger("Registering LCP...");

  vector<QueryArrayElement> registration;

  QueryArrayElement reg;
  reg.value = "GREGISTRATION_LCP";
  reg.type = "string";
  registration.push_back(reg);

  QueryArrayElement group;
  group.value = configLCP.GROUP;
  group.type = "string";
  registration.push_back(group);

  string regQuery = encoder(registration);

  logger("Generated registration query : ", regQuery);
  logger("Writing query to GCP, regSock: ", regSock);

  int writtenBytes = write(regSock, regQuery.c_str(), regQuery.length());
  if (writtenBytes == -1) {
    perror("LCP Registration error");
    exit(EXIT_FAILURE);
  };

  logger("Reading registration response");
  char response[10];

  int bytesRead = read(regSock, response, sizeof(response));
  if (bytesRead == 0) {
    logger("Error while registering, connection closed by GCP");
    exit(EXIT_FAILURE);
  } else if (bytesRead < 0) {
    perror("Error while reading registration response");
    close(regSock);
    exit(EXIT_FAILURE);
  } else {
    response[bytesRead] = '\0';
    string res(response);
    DecodedResponse msg = responseDecoder(res);
    if (msg.error.invalid) {
      logger("Failed LCP registration");
      logger("GCP response : ", res);
      close(regSock);
      exit(EXIT_FAILURE);
    }

    logger("Registered with GCP, response : ", response);
  }

  return 0;
}

vector<QueryArrayElement> connRegisterationMessage(string type, string lcpId) {
  vector<QueryArrayElement> registration;

  QueryArrayElement reg;
  reg.value = "GREGISTRATION_CONNECTION";
  reg.type = "string";
  registration.push_back(reg);

  QueryArrayElement group;
  group.value = configLCP.GROUP;
  group.type = "string";
  registration.push_back(group);

  QueryArrayElement lcp;
  lcp.value = lcpId; // Use this LCP unique ID
  lcp.type = "string";
  registration.push_back(lcp);

  QueryArrayElement connectionType;
  connectionType.value = type;
  connectionType.type = "string";
  registration.push_back(connectionType);

  return registration;
}

int connectionRegistration(int connSockFd, string type, string lcpId) {
  logger("Registration LCP connection with GCP, connSockFd: ", connSockFd,
         " type : ", type);

  // GREGISTRATION_CONNECTION groupName lcpName typeOfConnection
  vector<QueryArrayElement> registration =
      connRegisterationMessage(type, lcpId);

  string regQuery = encoder(registration);

  logger("Generated LCP connection registration query : ", regQuery,
         " type : ", type);
  logger("Writing query to GCP, connSockFd: ", connSockFd, "  type ", type);

  int writtenBytes = write(connSockFd, regQuery.c_str(), regQuery.length());
  if (writtenBytes == -1) {
    perror("LCP Connection Registration error");
    return -1;
  };
  logger("Type : ", type, " fd : ", connSockFd,
         "Written bytes : ", writtenBytes);

  logger("Reading LCP connection registration response for connSockFd : ",
         connSockFd, " type : ", type);
  char response[10];

  int bytesRead = read(connSockFd, response, sizeof(response));
  if (bytesRead == 0) {
    logger("Error while registering LCP connection, connection closed by GCP, "
           "type : ",
           type);
    return -1;
  } else if (bytesRead < 0) {
    logger("Error while reading registration response, connSockFd : ",
           connSockFd, " type : ", type);
    perror("Error while reading registration response");
    close(connSockFd);
    return -1;
  } else {
    response[bytesRead] = '\0';
    string res(response);
    DecodedResponse msg = responseDecoder(res);
    if (msg.error.invalid) {
      logger("Failed LCP connection registration, type : ", type,
             "connSockFd : ", connSockFd);
      logger("GCP response : ", res);
      close(connSockFd);
      return -1;
    }

    logger("Registered LCP connection with GCP, type : ", type,
           " response : ", response);
  }
  return 0;
}
