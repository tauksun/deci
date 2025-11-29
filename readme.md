# DECI – Distributed Eventually Consistent In‑Memory Cache

Keep cache access local and fast, while synchronizing data across service instances.

---

## Why DECI?

Traditional microservice architectures often place a cache service on a separate server (like Elasticache), requiring each application instance to make network calls for cache operations. These network calls increase latency and undermine the speed benefits of in‑memory caching.

**DECI** (Distributed Eventually Consistent In‑Memory Cache) flips this paradigm: it keeps the cache directly alongside the application process on the same machine, truly leveraging the speed and efficiency of local memory.

![Traditional approach: microservices call a remote cache over the network, adding latency.](./doc/current-approach.svg)

---

## How DECI Works

For each application instance, a dedicated **LCP** (**L**ocal **C**ache **P**rocess) runs on the same server. The application communicates with the LCP via a fast UNIX socket, keeping cache access local and ultra‑fast.

![Each server runs an LCP, and the application accesses the cache locally via a UNIX socket.](./doc/App-LCP.svg)

A central **GCP** (**G**lobal **C**ache **P**rocess) maintains a global cache (similar to a traditional cache service accessible over the network) and also acts as the synchronizer. LCPs synchronize their local updates with GCP, which fans out changes to peer LCPs, keeping caches eventually consistent.

Global cache can be used for huge data, or for shared data required by multiple microservices.

![LCP provides fast local caching. GCP maintains a global cache accessed by LCPs via network and acts as the synchronizer.](./doc/LCP-GCP-basic.svg)

Each micro‑service creates its own group in GCP. All LCP instances for that service join the group. Any create, update, or delete performed by one instance is propagated to peers through GCP.

![Groups in GCP keep LCPs for the same service synchronized.](./doc/LCP-GCP.svg)

![Groups in GCP keep LCPs for the same service synchronized.](./doc/Sync.svg)

### Key properties

- **Low latency:** Cache operations happen in‑memory on the same machine.
- **Scalable:** Each application instance and LCP is independent, scaling horizontally.
- **Eventual consistency:** GCP ensures all peers see updated data after local changes.
- **Minimal network overhead:** Only synchronization and global cache queries go over the network.
- **Persistence and recovery:** GCP maintains a per‑group WAL (Write‑Ahead Log) and streams it to new LCPs so they can catch up.

---

## DECI Ecosystem

### Docker Images

Run DECI components as containers.

- **GCP image:** `tauksun/deci-gcp`  
  <https://hub.docker.com/r/tauksun/deci-gcp>
- **LCP image:** `tauksun/deci-lcp`  
  <https://hub.docker.com/r/tauksun/deci-lcp>

### Node.js Client

Connect your Node.js services to the local LCP using the `deci` client. It manages the UNIX socket and connection pool for you.

- npm package: `deci`  
  <https://www.npmjs.com/package/deci>

Install:

``` npm install deci ```

Client libraries for other languages are coming soon.

---

## Run as Containers

### Global Cache Process (GCP)

``` docker run --name gcp -d -v /tmp/gcp/log:/var/log/gcp -v /tmp/gcp/wal:/var/lib/gcp -p 7480:7480 tauksun/deci-gcp ```

### Local Cache Process (LCP)

``` docker run --name lcp -u $(id -u):$(id -g) -e USER_ID=$(id -u) -e GROUP_ID=$(id -g) -v /tmp:/tmp -v /tmp/lcp/log:/var/log/lcp --network=host tauksun/deci-lcp```

Adjust volume mounts and environment variables as per your environment and cache configuration.

More details:

- GCP Docker image: <https://hub.docker.com/r/tauksun/deci-gcp>  
- LCP Docker image: <https://hub.docker.com/r/tauksun/deci-lcp>

---

## Build from Source

Example build steps on Ubuntu (adapt packages/commands for your distribution):

```
sudo apt-get update
sudo apt-get install -y build-essential clang make

Clone and build
git clone https://github.com/tauksun/deci.git
cd deci

Build GCP
make gcp

Build LCP
make lcp



### Example configuration files

#### `gcp.conf`

```
MAX_CONNECTIONS = 1000
MAX_READ_BYTES = 1023
MAX_WRITE_BYTES = 1023
SERVER_PORT = 7480
```

#### `lcp.conf`
```
GROUP = deci # GCP Group Name
MAX_CONNECTIONS = 100
LCP = lcp # LCP Name
sock = /tmp/lcp.sock
SOCKET_REUSE = 1
MAX_READ_BYTES = 1023
MAX_WRITE_BYTES = 1023
healthUpdateTime = 3 # Seconds
MAX_SYNC_MESSAGES = 100
MAX_SYNC_CONNECTIONS = 100 # Receiving sync operations from GCP
GCP_SERVER_IP = 127.0.0.1
GCP_SERVER_PORT = 7480
MAX_GCP_CONNECTIONS = 100
```

## Project Status

Alpha – actively developed and being polished for external adoption.

---

## Contributing

Contributions, issues, and feature requests are welcome! Please open an issue to discuss your proposal.

---

## License

MIT

---
