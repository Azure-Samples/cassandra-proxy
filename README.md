# Cassandra Proxy

A dual write proxy for Cassandra to aid in data migrations.

## Features

This proxy handles client connections and forwards them to *two* Cassandra
Cluster simultaneously. The idea is that writes will be delivered to both
clusters thus allowing migrations without implementing dual write in the
application.

**Note:** The proxy proxies between two servers in a respective cluster. For optimal performance
it is advised to run multiple proxies.

### Quickstart
(Add steps to get up and running quickly)

1. git clone https://github.com/Azure-Samples/cassandra-proxy.git
2. cd cassandra-proxy
3. mvn package
4. java -jar target/cassandra-proxy-1.0-SNAPSHOT-fat.jar <source-server> <destination-server>
5. cqlsh  -u <user> -p <password> localhost 29042

### Plans
* Read Reports
* Force protocol downgrades
* More robust TLS:wq
  
* ...

### Migration 
1. Run the proxy
2. Use any offline migration, e.g. Spark, sstableloader, or other.
3. TBD: Retrive read reports from proxy and see how close the results are

## Resources

TBD