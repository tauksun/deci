SHELL := /bin/bash

.PHONY: lcp gcp

CXXFLAGS = -std=c++17 -Wall -O2

all:
	@echo "Please specify what to build (lcp or gcp)"

lcp:
	@echo "Building LCP..."
	clang++ $(CXXFLAGS) ./src/lcp/lcp.cpp ./src/lcp/server.cpp ./src/common/logger.cpp ./src/common/makeSocketNonBlocking.cpp ./src/common/decoder.cpp ./src/lcp/globalCacheOps.cpp ./src/lcp/synchronizationOps.cpp ./src/lcp/connect.cpp ./src/common/responseDecoder.cpp ./src/common/operate.cpp ./src/common/encoder.cpp ./src/lcp/registration.cpp ./src/common/randomId.cpp ./src/common/configParser.cpp ./src/lcp/config.cpp ./src/lcp/wal.cpp ./src/common/wal.cpp -o ./build/lcp

gcp:
	@echo "Building GCP..."
	clang++ $(CXXFLAGS) ./src/gcp/gcp.cpp ./src/gcp/group.cpp ./src/gcp/health.cpp ./src/gcp/server.cpp ./src/common/decoder.cpp ./src/common/encoder.cpp ./src/common/logger.cpp ./src/common/makeSocketNonBlocking.cpp ./src/common/operate.cpp ./src/common/responseDecoder.cpp ./src/common/randomId.cpp ./src/common/configParser.cpp ./src/gcp/config.cpp ./src/common/wal.cpp ./src/gcp/wal.cpp -o ./build/gcp
