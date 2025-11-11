#include "server.hpp"
#include "../common/config.hpp"
#include "../common/decoder.hpp"
#include "../common/encoder.hpp"
#include "../common/logger.hpp"
#include "../common/makeSocketNonBlocking.hpp"
#include "../common/operate.hpp"
#include "config.hpp"
#include "group.hpp"
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <unordered_map>

unordered_map<string, CacheValue> cache;

bool isSyncMessage(string &op) {
  if (op == "SET" || op == "DEL") {
    return true;
  }
  return false;
}

void epollIO(int epollFd, int socketFd, struct epoll_event &ev,
             struct epoll_event *events, int timeout,
             std::deque<ReadSocketMessage> &readSocketQueue) {

  logger("Server : In epollIO");
  int readyFds =
      epoll_wait(epollFd, events, configGCP.MAX_CONNECTIONS, timeout);

  logger("Server : Looping on readyFds : ", readyFds);

  if (readyFds == -1) {
    perror("epoll_wait error");
    exit(EXIT_FAILURE);
  }

  for (int n = 0; n < readyFds; ++n) {
    if (events[n].data.fd == socketFd) {
      logger("Server : Accepting client connection");

      while (true) {
        logger("Server : In while loop > accepting connections");
        int connSock = accept(socketFd, NULL, NULL);
        if (connSock == -1) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // No more connections to accept
            logger("Server : Read EAGAIN on socketFd : ", socketFd,
                   " breaking from while");
            break;
          } else {
            logger("Server : Error while accepting connection on socketFd : ",
                   socketFd, " connSock : ", connSock);
            perror("Server : accept");
            continue;
          }
        }

        logger("Server : Connection accepted : ", connSock);
        logger("Server : Making connection non-blocking : ", connSock);
        if (makeSocketNonBlocking(connSock)) {
          perror("fcntl socketFd");
          // TODO: Check if client connection needs to be  handled differently
          close(connSock);
          continue;
        }

        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = connSock;

        logger("Server : Adding connection : ", connSock,
               " for monitoring by epoll");
        if (epoll_ctl(epollFd, EPOLL_CTL_ADD, connSock, &ev) == -1) {
          perror("epoll_ctl: connSock");
          close(connSock);
        }
      }
    } else {
      // Add to readSocketQueue
      logger("Server : Adding to readSocketQueue : ", events[n].data.fd);
      ReadSocketMessage sock;
      sock.fd = events[n].data.fd;
      sock.data = "";
      sock.readBytes = 0;
      readSocketQueue.push_back(sock);
    }
  }
}

void readFromSocketQueue(std::deque<ReadSocketMessage> &readSocketQueue,
                         std::deque<Operation> &operationQueue,
                         std::deque<WriteSocketMessage> &writeSocketQueue,
                         unordered_map<std::string, GroupQueueEventFd> &groups,
                         unordered_map<int, FdGroupLCP> &fdGroupLCPMap,
                         int epollFd) {
  // Read few bytes
  // If socket still has data, re-queue

  logger("Server : In readFromSocketQueue");
  long pos = 0;
  long currentSize = readSocketQueue.size();

  // Group's eventFd map for which message is pushed to concurrent queue
  unordered_map<string, int> groupEventFds;

  while (pos < currentSize) {
    char buf[1024];
    ReadSocketMessage msg = readSocketQueue.front();

    int readBytes = read(msg.fd, buf, configGCP.MAX_READ_BYTES);
    if (readBytes == 0) {
      // Connection closed by peer
      close(msg.fd);
      continue;
    } else if (readBytes < 0) {
      logger("Server : readBytes < 0, for fd : ", msg.fd);
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // No data now, skip
        logger("Server : Received EAGAIN for fd : ", msg.fd);
      } else {
        // Other error, clean up
        logger("Server : Error while reading, closing fd : ", msg.fd);
        perror("Server : Error while reading from socket");
        close(msg.fd);
      }
    } else if (readBytes > 0) {
      msg.readBytes += readBytes;
      msg.data.append(buf, readBytes);
    }

    if (readBytes == configGCP.MAX_READ_BYTES) {
      // more data to read, Re-queue
      logger("Server : Read max bytes > re-queuing, fd : ", msg.fd);
      readSocketQueue.push_back(msg);
    } else if (readBytes > 0) {
      // Parse the message here & push to operationQueue
      DecodedMessage parsed = decoder(msg.data);
      if (parsed.error.partial) {
        // If message is parsed partially, re-queue in readSocketQueue
        logger("Server : Partially parsed, re-queuing");
        logger("Server : TEMP : msg : ", msg.data, " readBytes : ", readBytes);
        readSocketQueue.push_back(msg);
      } else if (parsed.error.invalid) {
        logger("Server : Invalid message : ", msg.data);
        string res = "Invalid Message";
        WriteSocketMessage errorMessage;
        errorMessage.fd = msg.fd;
        errorMessage.response = encoder(&res, "error");
        writeSocketQueue.push_back(errorMessage);

        // Re-queue for event loop to read this till EAGAIN
        logger(
            "Server : Re-queue for event loop to read this till EAGAIN, fd : ",
            msg.fd);
        msg.data = "";
        msg.readBytes = 0;
        readSocketQueue.push_back(msg);
      } else {
        logger("Server : Successfully parsed");
        if (isSyncMessage(parsed.operation)) {
          // Sync operation > push in group's concurrent queue
          auto val = fdGroupLCPMap.find(msg.fd);
          if (val == fdGroupLCPMap.end()) {
            logger("Server : Invalid msg.fd : ", msg.fd);
            close(msg.fd);
            continue;
          }

          FdGroupLCP sock = val->second;

          logger("Server : Extracting socket group concurrent queue & eventFd");
          auto sockGroupData = groups.find(sock.group);
          if (sockGroupData == groups.end()) {
            logger("Server : Couldn't find socket group data, socket : ",
                   msg.fd, " group : ", sock.group);
            close(msg.fd);
            continue;
          }

          GroupConcurrentSyncQueueMessage queueMsg;
          queueMsg.fd = msg.fd;
          queueMsg.lcp = sock.lcp;
          queueMsg.query = msg.data;

          logger("Server : Pushing message to concurrent queue for group : ",
                 sock.group);

          sockGroupData->second.queue.enqueue(queueMsg);
          groupEventFds[sock.group] = sockGroupData->second.eventFd;
        } else if (parsed.operation == "GREGISTRATION_LCP") {
          auto grp = groups.find(parsed.reg.group);
          string res;
          if (grp != groups.end()) {
            logger("Server : Group : ", grp->first, " already exists");
            res = "0";
          } else {
            logger(
                "Server : Creating new group with ConcurrentQueue & eventFd : ",
                parsed.reg.group);

            groups.emplace(
                parsed.reg.group,
                GroupQueueEventFd{moodycamel::ConcurrentQueue<
                                      GroupConcurrentSyncQueueMessage>(),
                                  eventfd(0, EFD_NONBLOCK)});
            logger("Server : Created group : ", parsed.reg.group,
                   " starting its thread...");

            GroupQueueEventFd &groupData = groups[parsed.reg.group];
            std::thread grpThread(group, groupData.eventFd,
                                  std::ref(groupData.queue));
            grpThread.detach();
            logger("Server : Started thread for group");
            res = "1";
          }

          logger("Server : Responding to LCP registration with : ", res);
          WriteSocketMessage response;
          response.fd = msg.fd;
          response.response = encoder(&res, "integer");
          writeSocketQueue.push_back(response);
        } else if (parsed.operation == "GREGISTRATION_CONNECTION") {
          // From the perspective of LCP
          // Type of a connection can be : receiver, sender, health

          // Group should already exists at this point for each connection
          // as it is created in GREGISTRATION_LCP
          //
          // for type : receiver
          //  Remove from current epoll monitoring
          //
          //  for type : sender
          //  Add to fd-group-lcp hashmap
          if (parsed.reg.type == configCommon::RECEIVER_CONNECTION_TYPE) {

            // Remove from server thread epoll monitoring
            logger(
                "Server : Removing from server thread epoll monitoring, fd : ",
                msg.fd);
            if (epoll_ctl(epollFd, EPOLL_CTL_DEL, msg.fd, nullptr) == -1) {
              logger("Server : Error while removing from server thread "
                     "monitoring, fd : ",
                     msg.fd, " group : ", parsed.reg.group);
              perror("removing from monitoring error epoll_ctl");
              logger("Server : Closing connection for fd : ", msg.fd);
              close(msg.fd);
              continue;
            }

            // Push to Group ConcurrentQueue for registration
            logger(
                "Server : Adding to ConcurrentQueue for registration, Group : ",
                parsed.reg.group, " fd : ", msg.fd);
            GroupConcurrentSyncQueueMessage regMessage;
            regMessage.lcp = parsed.reg.lcp;
            regMessage.fd = msg.fd;
            regMessage.connectionRegistration = true;
            groups[parsed.reg.group].queue.enqueue(regMessage);
            logger("Server : Pushed fd : ", msg.fd,
                   " to ConcurrentQueue of Group : ", parsed.reg.group);

            groupEventFds[parsed.reg.group] = groups[parsed.reg.group].eventFd;
          } else {
            logger("Server : Adding to fdGroupLCPMap, fd : ", msg.fd);
            FdGroupLCP fdData;
            fdData.group = parsed.reg.group;
            fdData.lcp = parsed.reg.lcp;
            fdGroupLCPMap[msg.fd] = fdData;

            logger("Server : Queuing response for LCP connection "
                   "registration "
                   "to writeSocketQueue of type : ",
                   configCommon::SENDER_CONNECTION_TYPE, " fd : ", msg.fd);
            string res = "1";
            WriteSocketMessage response;
            response.fd = msg.fd;
            response.response = encoder(&res, "integer");
            writeSocketQueue.push_back(response);
          }

        } else {
          // Push to operation queue
          logger("Server : Pushing to operation queue, op : ",
                 parsed.operation);
          Operation op;
          op.fd = msg.fd;
          op.msg = parsed;
          operationQueue.push_back(op);
        }

        // Re-queue for event loop to read this till EAGAIN
        logger(
            "Server : Re-queue for event loop to read this till EAGAIN, fd : ",
            msg.fd);
        msg.data = "";
        msg.readBytes = 0;
        readSocketQueue.push_back(msg);
      }
    }

    readSocketQueue.pop_front();
    pos++;
  }

  logger("Server : checking for triggering group eventFds");
  for (const auto &pair : groupEventFds) {
    logger("Server : Triggering eventFd for group : ", pair.first,
           " eventFd : ", pair.second);

    uint64_t counter = 1;
    write(pair.second, &counter, sizeof(counter));
  }
}

void performOperation(std::deque<ReadSocketMessage> &readSocketQueue,
                      std::deque<Operation> &operationQueue,
                      std::deque<WriteSocketMessage> &writeSocketQueue) {

  long pos = 0;
  long currentSize = operationQueue.size();

  logger("Server : In performOperation");

  while (pos < currentSize) {
    // Perform operation

    WriteSocketMessage response;
    logger("Server : Performing operation");
    Operation op = operationQueue.front();
    logger("Server : Op : ", op.msg.operation);
    operate(op, response, cache);

    // Send Response
    response.fd = op.fd;
    writeSocketQueue.push_back(response);

    operationQueue.pop_front();
    pos++;
  }
}

void writeToSocketQueue(std::deque<WriteSocketMessage> &writeSocketQueue) {
  // Write few bytes & keep writing till whole content is written

  long pos = 0;
  long currentSize = writeSocketQueue.size();

  logger("Server : In writeSocketQueue");

  while (pos < currentSize) {
    WriteSocketMessage response = writeSocketQueue.front();
    logger("Server : writeSocketQueue : fd : ", response.fd,
           " response : ", response.response);
    int writtenBytes = write(response.fd, response.response.c_str(),
                             response.response.length());

    logger("Server : writtenBytes : ", writtenBytes, " for fd : ", response.fd);
    writeSocketQueue.pop_front();
    pos++;
  }
}

void server(std::unordered_map<std::string, GroupQueueEventFd> &groups) {
  // Create
  logger("Server : Creating TCP server socket");
  int socketFd = socket(AF_INET, SOCK_STREAM, 0);
  if (socketFd == -1) {
    perror("Error while creating server socket");
    exit(EXIT_FAILURE);
  }

  int yes = 1;
  logger("Server : Making socket re-usable : ", socketFd);
  if (setsockopt(socketFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
    perror("setsockopt(SO_REUSEADDR) failed");
    exit(EXIT_FAILURE);
  }

  // Bind
  struct sockaddr_in add;
  add.sin_family = AF_INET;
  add.sin_port = htons(configGCP.SERVER_PORT);
  add.sin_addr.s_addr = INADDR_ANY;

  int bindResult = bind(socketFd, (struct sockaddr *)&add, sizeof(add));
  logger("Server : bindResult : ", bindResult);
  if (bindResult != 0) {
    perror("Error while binding to socket");
    exit(EXIT_FAILURE);
  }

  // Listen
  logger("Server : Starting listening on : ", socketFd);
  int listenResult = listen(socketFd, configGCP.MAX_CONNECTIONS);
  logger("Server : listenResult : ", listenResult);
  if (listenResult != 0) {
    perror("Error while listening on socket");
    exit(EXIT_FAILURE);
  }

  // Accept (E-Poll)
  struct epoll_event ev, events[configGCP.MAX_CONNECTIONS];

  logger("Server : Making server socket non-blocking : ", socketFd);
  if (makeSocketNonBlocking(socketFd)) {
    perror("fcntl socketFd");
    exit(EXIT_FAILURE);
  }

  int epollFd = epoll_create1(0);
  if (epollFd == -1) {
    perror("epoll create error");
    exit(EXIT_FAILURE);
  }

  // Configure Edge triggered
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = socketFd;

  // Add socket descriptor for monitoring
  if (epoll_ctl(epollFd, EPOLL_CTL_ADD, socketFd, &ev) == -1) {
    perror("epoll_ctl socketFd");
    exit(EXIT_FAILURE);
  }

  int timeout = 0;
  std::deque<ReadSocketMessage> readSocketQueue;
  std::deque<Operation> operationQueue;
  std::deque<WriteSocketMessage> writeSocketQueue;
  unordered_map<int, FdGroupLCP> FdGroupLCPMap;

  logger("Server : Starting event loop");
  while (1) {

    /**
     * epoll_wait
     * Read from socket queue
     * perform operation on parsed messages
     * write response using socket queue
     *
     * */

    epollIO(epollFd, socketFd, ev, events, timeout, readSocketQueue);
    readFromSocketQueue(readSocketQueue, operationQueue, writeSocketQueue,
                        groups, FdGroupLCPMap, epollFd);
    performOperation(readSocketQueue, operationQueue, writeSocketQueue);
    writeToSocketQueue(writeSocketQueue);

    // If there are operations in read/write queue > keep timeout to be 0,
    // else -1(Infinity)
    if (readSocketQueue.size() || operationQueue.size() ||
        writeSocketQueue.size()) {
      timeout = 0;
    } else {
      timeout = -1;
    }

    logger("Server : Waiting for epoll with timeout : ", timeout);
  }
}
