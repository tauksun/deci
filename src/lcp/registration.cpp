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

  int regSock =
      establishConnection(configLCP::GCP_SERVER_IP, configLCP::GCP_SERVER_PORT);
  if (regSock == -1) {
    perror("Cannot establish connection with GCP for registration");
    exit(EXIT_FAILURE);
  }

  logger("Registration connection established, regSock: ", regSock);
  logger("Registering...");

  vector<QueryArrayElement> registration;

  QueryArrayElement reg;
  reg.value = "GREGISTRATION_LCP";
  reg.type = "string";
  registration.push_back(reg);

  QueryArrayElement group;
  group.value = configLCP::GROUP;
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
    close(regSock);
  }

  return 0;
}

