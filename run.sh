if [ "$1" == "" ]; then
    echo "Please provide what to build";
    exit -1;
fi

if [ $1 == "lcp" ]; then
echo "Building LCP...";
clang++ ./src/lcp/lcp.cpp ./src/lcp/server.cpp ./src/common/logger.cpp ./src/common/makeSocketNonBlocking.cpp ./src/common/decoder.cpp ./src/lcp/health.cpp ./src/lcp/globalCacheOps.cpp ./src/lcp/synchronizationOps.cpp ./src/lcp/connect.cpp ./src/common/responseDecoder.cpp ./src/common/operate.cpp ./src/common/encoder.cpp ./src/lcp/registration.cpp -o ./build/lcp
fi

if [ $1 == "gcp" ]; then
echo "Building GCP...";
clang++ ./src/gcp/gcp.cpp ./src/gcp/group.cpp ./src/gcp/health.cpp ./src/gcp/server.cpp ./src/common/decoder.cpp ./src/common/encoder.cpp ./src/common/logger.cpp ./src/common/makeSocketNonBlocking.cpp ./src/common/operate.cpp ./src/common/responseDecoder.cpp -o ./build/gcp
fi

