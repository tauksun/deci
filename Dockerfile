# Build Stage
FROM ubuntu:22.04 AS builder

# Install build tools and dependencies
RUN apt-get update && \
    apt-get install -y make gcc g++ clang bash && \
    rm -rf /var/lib/apt/lists/*
    
ARG TARGET
WORKDIR /app

# Copy source code and Makefile
COPY ./src /app/src
COPY Makefile /app

# Create build directory and compile
RUN mkdir -p /app/build
COPY ./build/${TARGET}.conf /app/build/
RUN make ${TARGET}

# Package (runtime) Stage
FROM ubuntu:22.04 AS runtime

WORKDIR /app

ARG TARGET=${TARGET}
COPY --from=builder /app/build/${TARGET} /app/${TARGET}
COPY --from=builder /app/build/${TARGET}.conf /app/${TARGET}.conf

ENTRYPOINT ["/bin/bash", "-c", "/app/${TARGET}"]
