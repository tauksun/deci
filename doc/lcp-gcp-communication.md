As cache is maintained locally 
in memory for each LCP 
deployed on a separate
machine, similarly a Global
cache is also maintained
(different than LCP), 
which can be used by 
application process using
GSET, GGET, GDEL like
commands via LCP. This global 
cache can be used to store
large values which can be 
inefficient to store for each LCP

Application process can send : 
GET, SET, DEL commands which operate on 
LCP level cache.
And commands like : GGET, GSET, GDEL 
operate on GCP level cache through LCP.

Each LCP has 2 threads :

Thread 1 : Establishes connection with GCP & maintains
a connection pool out of which 60% connections are used
to receive sync queries pushed by GCP to LCP. 

40% of connections are used by LCP to send GGET, 
GSET, GDEL queries & sync queries which are the
changes on this specific LCP.

Thread 2: This runs UNIX socket server which 
application process connects to, & all the fds are
monitored using edge triggerd epoll. 
This uses multiple queues to read from these
sockets, parse the messages, operate on cache, 
write back to application process.

For each SET commands which has a flag set for 
sync, also triggers an GSYNC command as this piece of 
cache information needs to be synced to other LCPs.
This GSYNC command is handled by Thread 1 as it has 
connection pool with GCP.

Thread 1 also runs a UNIX socket server, which Thread 2 
establishes connections (pool) with. These fds are monitored in Thread 1
with epoll. Whenever a SET request with a sync flag comes from 
application process at Thread 2 Server, Thread 2 writes to one of the 
socket connections to Thread 1, which triggers epoll & then Thread 1
internally uses a queue to handle these queries.
For each GSYNC query Thread 1 pushes it in a queue. -> Checks
if there is a connection available from LCP-GCP connection pool, 
if yes, then writes to it else, waits till any connection is available, 
then writes to it. 
Question : Is this good, efficient ? will it work ? Or there is a better way 
to achieve this ? 


Similarly, Thread 1 establishes another set of connection pool with
Thread2 UNIX socket server, whenever there is a sync request 
received on Thread 1 (60% connection pool) from GCP it pushes the same 
query to the Thread 2 socket connection, which is already monitored by epoll, 
then Thread 2 uses the same process as it processes the messages from 
application as there is no difference between message format.
