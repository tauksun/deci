# DECI – Distributed Eventually Consistent In-Memory Cache

**Ultra-fast, local cache with eventual consistency for microservices.**

---

## Why DECI?

Traditional microservice architectures often use a separate cache service (like ElastiCache), making every microservice instance perform network calls for cache operations. These network calls increase latency and undermine the key benefit of an in-memory cache.

![Traditional approach: Microservices access remote cache over the network, introducing latency.](./doc/current-approach.svg)

**DECI** (Distributed Eventually Consistent In-Memory Cache) flips this paradigm: it keeps the cache directly alongside the application process on the same machine, truly leveraging the speed and efficiency of local memory.

---

## How DECI Works

For each microservice instance, a dedicated **LCP** (Local Cache Process) runs on the same server. The application communicates with the LCP via a UNIX socket, keeping cache access local and ultra-fast.

![Each server runs an LCP, and the application accesses the cache locally via a UNIX socket.](./doc/App-LCP.svg)

---

## Overview Diagram

![LCP provides fast local caching. GCP maintains a global cache accessed by LCPs via network.](./doc/LCP-GCP-basic.svg)

---

## Distributed Synchronization and Global Cache

DECI introduces the **GCP** (Global Cache Process), running on another server. GCP maintains a global cache accessible by all LCPs. When the application modifies its local cache (using the LCP), changes are sent to the GCP, which then synchronizes these updates across peer LCPs of the same microservice, ensuring eventual consistency.

![LCPs use a pool of TCP connections to communicate with GCP for global cache access and synchronization.](./doc/LCP-GCP.svg)

---

## Synchronization in Microservices

Each microservice creates its own group in GCP. All LCP instances of that microservice join the group. Any create, update, or delete operation made in the LCP’s local cache is propagated via GCP to all the other LCPs in the group, ensuring all instances are eventually consistent.

![All LCPs of a service synchronize changes through GCP, keeping caches eventually consistent.](./doc/Sync.svg)

### Key Features

- **Low Latency:** Cache operations happen in-memory, on the local server.
- **Scalable:** Each application instance and LCP is independent, enabling horizontal scaling.
- **Eventual Consistency:** GCP ensures all peers see updated data after local changes.
- **Minimal Network Overhead:** Only synchronization and global cache queries use the network—cache hits are purely local.
- **Persistence and Recovery:** GCP maintains a WAL (Write-Ahead Log) file for each group, which is streamed to any newly joined LCP to bring it up-to-date with the current cache state.

## Running DECI

### Global Cache Process (GCP)

Docker Image: [tauksun/deci-gcp](https://hub.docker.com/r/tauksun/deci-gcp)

```sudo docker run --name gcp -v /tmp/gcp:/tmp/gcp -v /tmp/gcp/gcp.conf:/app/gcp.conf -e TARGET=gcp -p 7480:7480 tauksun/deci-gcp```


### Local Cache Process (LCP)

Docker Image: [tauksun/deci-lcp](https://hub.docker.com/r/tauksun/deci-lcp)

```sudo docker run -u $(id -u):$(id -g) -e USER_ID=$(id -u) -e GROUP_ID=$(id -g) --name lcp -v /tmp/lcp:/tmp/lcp -v /tmp/lcp/lcp.conf:/app/lcp.conf -e TARGET=lcp --network=host tauksun/deci-lcp```


*Adjust volume mounts and environment variables as per your environment and cache configuration.*

---

## Upcoming Release

- Documentation
- LCP shutdown handling
- Stateful incremental parser
- TTL implementation
- SSL

---

## Project Status

Alpha – actively developed and being polished for external adoption.

---

## Contributing

Contributions, issues, and feature requests are welcome! Please open an issue to discuss your proposal.

---

## License

MIT

---
