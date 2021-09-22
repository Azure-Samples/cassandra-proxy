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


## Quickstart
(Add steps to get up and running quickly assuming you have java and maven installed)

1. git clone https://github.com/Azure-Samples/cassandra-proxy.git
2. cd cassandra-proxy
3. mvn package
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
* Proxy needs to run on the same host as the source Cassandra if errors and crazyness needs to be avoided

## Resources

TBD
