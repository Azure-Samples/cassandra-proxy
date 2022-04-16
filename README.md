# Cassandra Proxy

A dual write proxy for Cassandra to aid in data migrations.

## Features

This proxy handles client connections and forwards them to *two* Cassandra
Clusters simultaneously. The idea is that writes will be delivered to both
clusters thus allowing migrations without implementing dual write in the
application.

**Note:** 
* The proxy proxies between two servers in a respective cluster. For optimal performance
it is advised to run multiple proxies.
* setting `--wait` to false will cause the proxy to not wait for a result from both servers but return once it gets 
  a result from the source sever. This obviously can lead to botched migrations if the request
  never reaches the target server for any reason. 

## ApacheCon Presentation
To see it in action watch our ApacheCon presentation:
[![IMAGE ALT TEXT](https://img.youtube.com/vi/fJIkV44p2Cs/0.jpg)](https://youtu.be/fJIkV44p2Cs "Cassandra Data Migration with Dual Write Proxy - German Eichberger")
## Quickstart
(Add steps to get up and running quickly assuming you have java and maven installed)

To build the proxy yourself run:
1. git clone https://github.com/Azure-Samples/cassandra-proxy.git
2. cd cassandra-proxy
3. mvn package

Alternatively, you can download the jar from the release section: https://github.com/Azure-Samples/cassandra-proxy/releases/

Then run:
4. java -jar target/cassandra-proxy-1.0-SNAPSHOT-fat.jar source-server destination-server
5. cqlsh  -u user -p password localhost 29042


## Docker
If you uncomment the jib plugin in the pom.xml `mvn package` will also create a (local) docker image. To use this run `docker run cosmos.microsoft.com/cassandra-proxy source destination`. You can push it to an adequate registry if needed on a different server.

## Monitoring
The system will log through slf4j/logback and thus allow customization of logs as needed

### Metrics
The system is using micrometer and provides a Prometheus compatible endpoint. Because micrometer is pluggable other metric aggregators could easily be supported if needed.

The metrics gathered are:

metric | type | Description
--- | --- | ---
cassandraProxy.cqlOperation.proxyTime | nanoseconds | time spend solely for proxy processing
cassandraProxy.cqlOperation.timer | nanoseconds | time spend for the whole request (includes waiting for a response from both C* servers)
cassandraProxy.cqlOperation.cqlServerErrorCount | counter | counts the occurrence of error responses from the server and proxy
cassandraProxy.cqlOperation.cqlDifferentResultCount | counter | counts when the result of the same cql operation differed between the servers
cassandraProxy.clientSocket.paused | nanoseconds | time we need to pause requests to give the client time to catch up
cassandraProxy.serverSocket.paused | nanoseconds | time we need to pause requests to give Cassandra time to catch up

Some also contain tags indicating the opcode from the CQL protocol and a more readable form
describing the request (e.g. Query).  

The pause metrics also include a server or client address and in the case of the server a user defined identifier.

## Ghost Proxy
If you choose to run the proxy on a different host than the source cassandra, you will need to set up
a substitution for the ips in `system.peers` so the cassandra client can loadbalance beween several proxies
and does not connect to a source cassandra directly. 

This can be done with `--ghostIps '{"10.25.0.12":"192.168.1.45"}` which will replace the ip of the source cassandra
`10.25.0.12` with the ip of a proxy running on `192.168.1.45`

This feature can also be leveraged to minimize risk in a migration to quickly switch back and fort between source
and destination cassandra.

## Production settings
Since the proxy opens two new connections for each client connections it's advisable
to increase the `ulimit` for open files for the user who is running cassandra-proxy

Other useful settings when starting the proxy are:

|Parameter |Description |Comment |
|---------|-----------|-------|
|--threads 10 | launches more than 1 thread | Increasing this cuts back on latency |
|--write-buffer-size 2097152 | size of write buffer for client in bytes | the default of 64KB might throttle if big objects are used |
|--tcp-idle-time-out |  TCP Idle Time Out in s (0 for infinite) | set this to the same value as the clients and the server. A mismatch might lead to not closing all the connections in a timely manner and running out of sockets |


## Future Plans
* Read Reports
* More robust TLS
* better docs
* ...

## Migration 
1. Run the proxy
2. Use any offline migration, e.g. Spark, sstableloader, or other.
3. TBD: Retrieve read reports from proxy and see how close the results are

## Known Issues
* The data centers need to be named the same (which is an issue with loadbalancing clients beginning 4.0)
  * This is especially difficult for schema changes
* Metrics are WIP - we noticed that some C* versions/implementations pad results differently
* TLS on the backend servers doesn't do hostname validation nor client certs nor...

## Resources

TBD

## How to make a release (contributors only)
1. Tag what you want to release `git tag v1.0.0` 
2. Push the tags `git push upstream main --tags` (you might have named repos differently)
3. In github.com click "New Release" and select the tag
4. Upload the jar you build before from your workstation. Make sure to wait until it's fully uploaded
5. Submit
