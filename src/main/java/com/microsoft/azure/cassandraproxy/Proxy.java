/*
 * Copyright Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.azure.cassandraproxy;

import com.datastax.oss.protocol.internal.*;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.Rows;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.cli.*;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.*;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;

import java.util.regex.Pattern;


/*-h localhost localhost --proxy-pem-keyfile /home/german/Project/cassandra-proxy/src/main/resources/server.pem --proxy-pem-certfile /home/german/Project/cassandra-proxy/src/main/resources/server.key*/
public class Proxy extends AbstractVerticle {
    public static final String UUID = "UUID()";
    public static final String NOW = "NOW()";
    private static final Logger LOG = LoggerFactory.getLogger(Proxy.class);
    public static final String CASSANDRA_SERVER_PORT = "29042";
    public static final String PROTOCOL_VERSION = "protocol-version";
    private static final String PEERS = "SYSTEM.PEERS";
    private static final String PEERS_V2 = "SYSTEM.PEERS_V2" ;
    private static CommandLine commandLine;
    private BufferCodec bufferCodec = new BufferCodec();
    private FrameCodec<BufferCodec.PrimitiveBuffer> serverCodec = FrameCodec.defaultServer(bufferCodec, Compressor.none());
    private FrameCodec<BufferCodec.PrimitiveBuffer> clientCodec = FrameCodec.defaultClient(bufferCodec, Compressor.none());
    private final UUIDGenWrapper uuidGenWrapper;
    private Credential credential;
    private Pattern pattern;
    private Set<byte[]> filterPreparedQueries = new ConcurrentHashSet<>();
    private Map<InetAddress, InetAddress> ghostProxyMap = new HashMap<>();


    public Proxy() {
        this.uuidGenWrapper = new UUIDGenWrapper();
    }

    //for tests
    public Proxy(UUIDGenWrapper uuidGenWrapper, FrameCodec<BufferCodec.PrimitiveBuffer> serverCodec, FrameCodec<BufferCodec.PrimitiveBuffer> clientCodec) {
        this.uuidGenWrapper = uuidGenWrapper;
        this.serverCodec = serverCodec;
        this.clientCodec = clientCodec;
    }

    public static void main(String[] args) {
        CLI cli = CLI.create("cassandra-proxy")
                .setSummary("A dual write proxy for cassandra.")
                .addOption(
                        new Option().setLongName("help").setShortName("h").setFlag(true).setHelp(true))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("wait")
                        .setShortName("W")
                        .setDescription("wait for write completed on both clusters")
                        .setDefaultValue("true"))
                .addArgument(new Argument()
                        .setDescription("Source cluster. This is the cluster which is authoritative for reads")
                        .setRequired(true)
                        .setArgName("source"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Source cluster port. This is the cluster which is authoritative for reads")
                        .setLongName("source-port")
                        .setDefaultValue("9042"))
                .addOption(new Option()
                        .setDescription("Source cluster identifier. This is an identifier used in logs and metrics for the source cluster")
                        .setLongName("source-identifier")
                        .setDefaultValue("source node"))
                .addArgument(new Argument()
                        .setRequired(true)
                        .setDescription("Destination cluster. This is the cluster we ignore reads for.")
                        .setArgName("target"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Destination cluster port. This is the cluster we ignore reads for.")
                        .setLongName("target-port")
                        .setDefaultValue("9042"))
                .addOption(new Option()
                        .setDescription("Target cluster identifier. This is an identifier used in logs and metrics for the target cluster")
                        .setLongName("target-identifier")
                        .setDefaultValue("target node"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Port number the proxy listens under")
                        .setLongName("proxy-port")
                        .setShortName("p")
                        .setDefaultValue(CASSANDRA_SERVER_PORT))
                .addOption(new Option()
                        .setDescription("Pem file containing the key for the proxy to perform TLS encryption. If not set, no encryption ")
                        .setLongName("proxy-pem-keyfile"))
                .addOption(new Option()
                        .setDescription("Pem file containing the server certificate for the proxy to perform TLS encryption. If not set, no encryption ")
                        .setLongName("proxy-pem-certfile"))
                .addOption(new Option()
                        .setDescription("Jks containing the key for the proxy to perform TLS encryption. If not set, no encryption ")
                        .setLongName("proxy-jks-file"))
                .addOption(new Option()
                        .setDescription("Password for the Jks store specified (Default: changeit)")
                        .setLongName("proxy-jks-password")
                        .setDefaultValue("changeit"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("How many threads should be launched")
                        .setLongName("threads")
                        .setShortName("t")
                        .setDefaultValue("1"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Supported Cassandra Protocol Version(s). If not set return what source server says")
                        .setLongName(PROTOCOL_VERSION)
                        .setMultiValued(true))
                .addOption(new Option()
                        .setDescription("Supported Cassandra CQL Version(s). If not set return what source server says")
                        .setLongName("cql-version")
                        .setMultiValued(true))
                .addOption(new Option()
                        .setDescription("Supported Cassandra Compression(s). If not set return what source server says")
                        .setLongName("compression")
                        .setMultiValued(true))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("compression-enabled")
                        .setDescription("If set, cassandra's compression is disabled")
                        .setDefaultValue("true"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("uuid")
                        .setDescription("scan for uuid and generate on proxy for inserts/updates")
                        .setDefaultValue("true"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("metrics")
                        .setDescription("provide metrics and start metrics server")
                        .setDefaultValue("true"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("only-message")
                        .setDescription("some C* APPIs will add payloads and warnings to each request (e.g. charges). Setting this to true will only consider the message for cqlDifferentResultCount metric ")
                        .setDefaultValue("true"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Port number the promethwus metrics are available")
                        .setLongName("metrics-port")
                        .setShortName("mp")
                        .setDefaultValue("28000"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("TCP Idle Time Out in s (0 for infinite)")
                        .setLongName("tcp-idle-time-out")
                        .setDefaultValue("0"))
                .addOption(new Option()
                        .setDescription("Target username if different credential from source. The system will use this user/pwd instead.")
                        .setLongName("target-username"))
                .addOption(new Option()
                        .setDescription("Target password if different credential from source. The system will use this user/pwd instead.")
                        .setLongName("target-password"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("disable-source-tls")
                        .setDescription("disable tls encryption on the source cluster")
                        .setDefaultValue("false"))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("disable-target-tls")
                        .setDescription("disable tls encryption on the source cluster")
                        .setDefaultValue("false"))
                .addOption(new Option()
                        .setDescription("Regex of queries to be filtered out and not be forwarded to target, e.g. 'insert into test .*'")
                        .setLongName("filter-tables"))
                .addOption(new TypedOption<Boolean>()
                    .setType(Boolean.class)
                    .setLongName("debug")
                    .setDescription("enable debug logging")
                    .setDefaultValue("false"))
                .addOption(new Option()
                        .setLongName("ghostIps")
                        .setDescription("list ips to be replaced, e.g. {\"10.25.0.2\":\"192.168.1.4\",...} "
                                + " - this will replace the ip in system.peers with the other one in queries to "
                                + "allow running the proxy on different ips from the source cluster"));

        // TODO: Add trust store, client certs, etc.

        try {
            commandLine = cli.parse(Arrays.asList(args));
        } catch (CLIException e) {
            System.out.println(e.getMessage());
            help(cli);
            System.exit(-1);
        }

        // The parsing does not fail and let you do:
        if (!commandLine.isValid() && commandLine.isAskingForHelp()) {
            help(cli);
            System.exit(-1);
        }

        for (Option o : cli.getOptions()) {
            if (o.getName().contains("password")) {
                LOG.info(o.getName() + " : " + "***");
            } else {
                LOG.info(o.getName() + " : " + commandLine.getOptionValue(o.getName()));
            }
        }

        for (Argument a : cli.getArguments()) {
            LOG.info(a.getArgName() + " : " + commandLine.getArgumentValue(a.getArgName()));
        }

        LOG.info("Cassandra Proxy starting...");

        // set log level
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        if ((Boolean)commandLine.getOptionValue("debug")) {
            root.setLevel(Level.DEBUG);
        } else {
            root.setLevel(Level.WARN);
        }

        VertxOptions options = new VertxOptions();
        //  Micrometer
        if ((Boolean)commandLine.getOptionValue("metrics")) {
            options.setMetricsOptions(
                    new MicrometerMetricsOptions()
                            .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true)
                                    .setStartEmbeddedServer(true)
                                    .setEmbeddedServerOptions(new HttpServerOptions().setPort(commandLine.getOptionValue("metrics-port"))))
                                    .setEnabled(true)
                                    .setJvmMetricsEnabled(true));
            LOG.info("Cassandra Proxy Metrics at port: {}", (Integer)commandLine.getOptionValue("metrics-port"));
        }

        Vertx vertx = Vertx.vertx(options);
        for (int i = 0; i < (Integer) commandLine.getOptionValue("threads"); i++) {
            vertx.deployVerticle(new Proxy());
        }
    }

    private static void help(CLI cli) {
        StringBuilder builder = new StringBuilder();
        cli.usage(builder);
        System.out.print(builder.toString());
    }

    @Override
    public void start() throws Exception {

        NetServerOptions options = new NetServerOptions().setPort(commandLine.getOptionValue("proxy-port"));
        if (commandLine.getOptionValue("proxy-pem-keyfile") != null && commandLine.getOptionValue("proxy-pem-certfile") != null && commandLine.getOptionValue("proxy-jks-file") == null) {
            PemKeyCertOptions pemOptions = new PemKeyCertOptions();
            pemOptions.addCertPath(commandLine.getOptionValue("proxy-pem-certfile"))
                    .addKeyPath(commandLine.getOptionValue("proxy-pem-keyfile"));
            options.setSsl(true).setPemKeyCertOptions(pemOptions);
        } else if (commandLine.getOptionValue("proxy-pem-keyfile") != null || commandLine.getOptionValue("proxy-pem-certfile") != null) {
            System.out.println("Both proxy-pem-keyfile and proxy-pem-certfile need to be set for TLS");
            LOG.error("Both proxy-pem-keyfile and proxy-pem-certfile need to be set for TLS");
            System.exit(-1);
        } else if (commandLine.getOptionValue("proxy-pem-keyfile") != null && commandLine.getOptionValue("proxy-pem-certfile") != null && commandLine.getOptionValue("proxy-jks-file") != null) {
            System.out.println("Only proxy-pem-keyfile and proxy-pem-certfile OR proxy-jks-file can to be set for TLS");
            LOG.error("Only proxy-pem-keyfile and proxy-pem-certfile OR proxy-jks-file can to be set for TLS");
            System.exit(-1);
        } else if (commandLine.getOptionValue("proxy-jks-file") != null) {
            JksOptions jksOptions = new JksOptions();
            jksOptions.setPath(commandLine.getOptionValue("proxy-jks-file"))
                    .setPassword(commandLine.getOptionValue("proxy-jks-password"));
            options.setSsl(true).setKeyStoreOptions(jksOptions);
        }

        String username = commandLine.getOptionValue("target-username");
        String password = commandLine.getOptionValue("target-password");
        if (username!=null && username.length()>0 && password != null && password.length()>0) {
            credential = new Credential(username, password);
        } else if ((username!=null && username.length()>0 ) || password != null && password.length()>0) {
            System.out.println("Both target-username and target-password need to be set if you have different accounts on the target system");
            LOG.error("Both target-username and target-password need to be set if you have different accounts on the target system");
            System.exit(-1);
        }

        List<String> protocolVersions = new ArrayList<>();
        if (commandLine.getOptionValues(PROTOCOL_VERSION) != null) {
            for (Object protocolVersion : commandLine.getOptionValues(PROTOCOL_VERSION)) {
                protocolVersions.add(protocolVersion + "/v" + protocolVersion);
            }
        }

        if (commandLine.getOptionValue("filter-tables") != null) {
            pattern = Pattern.compile(commandLine.getOptionValue("filter-tables"));
        }

        // set GhostProxy
        if (commandLine.getOptionValue("ghostIps") != null) {
            JsonObject object = null;
            try {
                object = new JsonObject((String) commandLine.getOptionValue("ghostIps"));
            } catch (Exception e) {
                LOG.error("Error parsing --ghostIps ", e);
            }
            for (Map.Entry<String, Object> ips : object) {
                if (!(ips.getValue() instanceof  String)) {
                    LOG.error("Can't parse ghotsIp" + object);
                    System.exit(1);
                }
                this.ghostProxyMap.put(InetAddress.getByName(ips.getKey()), InetAddress.getByName((String)ips.getValue()));
            }

        }

        int idleTimeOut = commandLine.getOptionValue("tcp-idle-time-out");
        options.setIdleTimeout(idleTimeOut);
        options.setIdleTimeoutUnit(TimeUnit.SECONDS);

        NetServer server = vertx.createNetServer(options);

        server.connectHandler(socket -> {
            ProxyClient client1 = new ProxyClient(commandLine.getOptionValue("source-identifier"), socket, protocolVersions, commandLine.getOptionValues("cql-version"), commandLine.getOptionValues("compression"), commandLine.getOptionValue("compression-enabled"),commandLine.getOptionValue("metrics"), commandLine.getOptionValue("wait"), null);
            Future c1 = client1.start(vertx, commandLine.getArgumentValue("source"), commandLine.getOptionValue("source-port"), !(Boolean)commandLine.getOptionValue("disable-source-tls"), idleTimeOut);
            ProxyClient client2 = new ProxyClient(commandLine.getOptionValue("target-identifier"),  (Boolean)commandLine.getOptionValue("metrics"), credential);
            Future c2 = client2.start(vertx, commandLine.getArgumentValue("target"), commandLine.getOptionValue("target-port"),  !(Boolean)commandLine.getOptionValue("disable-target-tls"), idleTimeOut);
            LOG.info("Connection to both Cassandra servers up)");
            FastDecode fastDecode = FastDecode.newFixed(socket, buffer -> {
                try {
                    final long startTime = System.nanoTime();
                    final int opcode = buffer.getByte(4);
                    FastDecode.State state = FastDecode.quickLook(buffer);
                    // Check if we support the protocol version
                    if (commandLine.getOptionValues(PROTOCOL_VERSION) != null && !commandLine.getOptionValues(PROTOCOL_VERSION).isEmpty()) {
                        int protocolVersion = buffer.getByte(0) & 0b0111_1111;
                        if (!isProtocolSupported(protocolVersion)) {
                            LOG.info("Downgrading Protocol from {}", protocolVersion);
                            writeToClientSocket(socket, client1, client2, errorProtocolNotSupported(buffer, startTime, opcode, state, protocolVersion));
                            return;
                        }
                    }

                    //Todo: Do we need to fake peers? Given that we would need to also come up with tokens that seems
                    // future work when C* is smart enough to deal with multiple C* on the same node but on different ports
                    // right now it will always connect to proxy if we set the proxy port even if there is C* running
                    // on another port.


                    if ((Boolean) commandLine.getOptionValue("uuid")
                            && state == FastDecode.State.query
                            && scanForUUID(buffer)) {
                        buffer = handleUUID(buffer);
                    }

                    boolean ghostProxy =  (!ghostProxyMap.isEmpty())

                            && state == FastDecode.State.query && (scanForPeers(buffer) || scanForPeersV2(buffer)) ;

                    boolean onlySource = false;
                    if (pattern != null
                            && ((state == FastDecode.State.query) || (state == FastDecode.State.prepare))) {
                        // we need to filter tables
                        BufferCodec.PrimitiveBuffer buffer2 = BufferCodec.createPrimitiveBuffer(buffer);
                        Frame r1 = serverCodec.decode(buffer2);
                        if (r1.message instanceof Query) {
                            Query q = (Query) r1.message;
                            if (pattern.matcher(q.query).matches()) {
                                onlySource = true;
                            }
                        } else if (r1.message instanceof Execute) {
                            Execute ex = (Execute) r1.message;
                            // is this a prepared statement for this table
                            if (filterPreparedQueries.contains(ex.queryId)) {
                                onlySource = true;
                            }
                        } else if (r1.message instanceof Prepare) {
                            Prepare p = (Prepare) r1.message;
                            if (pattern.matcher(p.cqlQuery).matches()) {
                                onlySource = true;
                            }
                        }
                    }

                    final long endTime = System.nanoTime();
                    Future<Buffer> f1 = client1.writeToServer(buffer).future();
                    if (!onlySource) {
                        Future<Buffer> f2 = client2.writeToServer(buffer).future();
                        CompositeFuture.all(f1, f2).onComplete(e -> {
                            Buffer buf = f1.result();

                            if (state == FastDecode.State.prepare && !(f1.result().equals(f2.result()))) {
                                // check if we need to substitute
                                BufferCodec.PrimitiveBuffer buffer2 = BufferCodec.createPrimitiveBuffer(buf);
                                Frame r1 = clientCodec.decode(buffer2);
                                buffer2 = BufferCodec.createPrimitiveBuffer(f2.result());
                                Frame r2 = clientCodec.decode(buffer2);
                                if ((r1.message instanceof Prepared) && (r2.message instanceof Prepared)) {
                                    Prepared res1 = (Prepared) r1.message;
                                    Prepared res2 = (Prepared) r2.message;
                                    if (res1.preparedQueryId != res2.preparedQueryId) {
                                        LOG.info("md5 of prepared statements differ between source and target -- need to substitute");
                                        client2.addPrepareSubstitution(res1.preparedQueryId, res2.preparedQueryId);
                                        LOG.debug("substituting {} for {}", res1.preparedQueryId, res2.preparedQueryId);
                                    }
                                }
                            }

                            if (ghostProxy && FastDecode.quickLook(buf) == FastDecode.State.result) {
                                buf = ghostProxySubstitution(buf);
                            }

                            if ((Boolean) commandLine.getOptionValue("metrics")) {
                                sendMetrics(startTime, opcode, state, endTime, f1, f2, buf);
                            }

                            if (commandLine.getOptionValue("wait")) {
                                // check if we got an error on the target for a prepared statement
                                if (checkUnpreparedTarget(state, f2.result())) {
                                    writeToClientSocket(socket, client1, client2, f2.result());
                                } else {
                                    writeToClientSocket(socket, client1, client2, buf);
                                }
                                // we waited for both results - now write to client

                            }
                        });
                    } else {
                        // only run those queries against the source to save roundtrip time
                        f1.onComplete(e -> {
                            Buffer buf = f1.result();
                            // TODO: metrics?
                            // Add the prepared id to the ones to filter
                            if (state == FastDecode.State.prepare) {
                                BufferCodec.PrimitiveBuffer buffer2 = BufferCodec.createPrimitiveBuffer(buf);
                                Frame r1 = clientCodec.decode(buffer2);
                                if (r1.message instanceof Prepared) {
                                    Prepared res1 = (Prepared) r1.message;
                                    filterPreparedQueries.add(res1.preparedQueryId);
                                }
                            }
                            if (commandLine.getOptionValue("wait")) {
                                writeToClientSocket(socket, client1, client2, buf);
                            }
                        });
                    }
                } catch (Exception e) {
                    LOG.error("Exception handling CQL packet.", e);
                }
            });
            fastDecode.endHandler(x -> {
                // Close client connections
                client1.close();
                client2.close();
                socket.close();
                LOG.info("Connection closed!");
            });
            fastDecode.exceptionHandler(x -> {
                // Close client connections
                client1.close();
                client2.close();
                socket.close();
                LOG.info("Connection closed!");
            });

        }).listen(res -> {
            if (res.succeeded()) {
                LOG.info("Server is now listening on  port: " + server.actualPort());
            } else {
                LOG.error("Failed to bind!");
                LOG.error(String.valueOf(res.cause()));
                System.exit(-1);
            }
        });

    }

    // we neeed to substitute the ips in system,peer with our own
    // TODO: test with system.peers_v2 !!!
    protected Buffer ghostProxySubstitution(Buffer buf) {
        BufferCodec.PrimitiveBuffer buffer2 = BufferCodec.createPrimitiveBuffer(buf);
        Frame r1 = clientCodec.decode(buffer2);

        if (r1.message instanceof Rows) {
            Rows res = (Rows) r1.message;
            if ((res.getMetadata().columnCount > 0)
                    && ("system".equalsIgnoreCase(res.getMetadata().columnSpecs.get(0).ksName))
                    && ("peers".equalsIgnoreCase(res.getMetadata().columnSpecs.get(0).tableName))) {
                List<ColumnSpec> columns = res.getMetadata().columnSpecs;
                Queue<List<ByteBuffer>> q = res.getData();
                Queue<List<ByteBuffer>> dst = new ArrayDeque<>();
                for (List<ByteBuffer> row : q) {
                    List<ByteBuffer> dstRow = new ArrayList<>();
                    for (int i = 0; i < res.getMetadata().columnCount; i++) {
                        ColumnSpec spec = columns.get(i);
                        if ("peer".equalsIgnoreCase(spec.name) || "rpc_address".equalsIgnoreCase(spec.name)) {
                            ByteBuffer bb = row.get(i);
                            if (bb == null) {
                                dstRow.add(bb);
                                continue;
                            }
                            byte[] b = new byte[4];
                            bb.get(b);
                            bb.rewind(); //reset to not mess up writing of the results later
                            try {
                                InetAddress ip = InetAddress.getByAddress(b);
                                if (this.ghostProxyMap.containsKey(ip)) {
                                    InetAddress ip2 = this.ghostProxyMap.get(ip);
                                    LOG.info("Replacing {} with {}", ip, ip2);
                                    byte[] bytes = ip2.getAddress();
                                    dstRow.add(ByteBuffer.wrap(bytes));
                                } else {
                                    LOG.error("Ip {} not found in ghost proxy configuration", ip);
                                    dstRow.add(bb);
                                }
                            } catch (UnknownHostException ex) {
                                ex.printStackTrace();
                            }
                        } else {
                            dstRow.add(row.get(i));
                        }
                    }
                    dst.add(dstRow);
                }
                Rows r = new DefaultRows(res.getMetadata(), dst);
                Frame f = Frame.forResponse(r1.protocolVersion, r1.streamId, r1.tracingId, r1.customPayload, r1.warnings, r);
                BufferCodec.PrimitiveBuffer b = serverCodec.encode(f);
                return b.buffer;
            }

        }

        return buf;
    }

    protected boolean checkUnpreparedTarget(FastDecode.State state, Buffer buf) {
        if (state == FastDecode.State.query && FastDecode.quickLook(buf) == FastDecode.State.error) {
            BufferCodec.PrimitiveBuffer buffer2 = BufferCodec.createPrimitiveBuffer(buf);
            Frame r1 = clientCodec.decode(buffer2);
            if (r1.message instanceof Error) {
                Error error = (Error) r1.message;
                if (error.code == ProtocolConstants.ErrorCode.UNPREPARED) {
                    // we got unrprepared from target so g
                    return true;
                }
            }
        }
        return false;
    }

    private Buffer errorProtocolNotSupported(Buffer buffer, long startTime, int opcode, FastDecode.State state, int protocolVersion) {
        StringBuilder supported = new StringBuilder("Invalid or unsupported protocol version (");
        supported = supported.append(protocolVersion).append("); supported versions are (");
        Iterator i = commandLine.getOptionValues(PROTOCOL_VERSION).iterator();
        while (i.hasNext()) {
            Object o = i.next();
            supported = supported.append(o).append("/v").append(o);
            if (i.hasNext()) {
                supported = supported.append(",");
            }
        }
        supported = supported.append(")");

        // generate a protocol error
        Error e = new Error(10, supported.toString());
        int streamId = buffer.getShort(2);
        Frame f = Frame.forResponse((Integer) commandLine.getOptionValues(PROTOCOL_VERSION).get(0), streamId, null, Collections.emptyMap(), Collections.emptyList(), e);
        if ((Boolean)commandLine.getOptionValue("metrics")) {
            MeterRegistry registry = BackendRegistries.getDefaultNow();
            Timer.builder("cassandraProxy.cqlOperation.proxyTime")
                    .tag("requestOpcode", String.valueOf(opcode))
                    .tag("requestState", state.toString()).register(registry)
                    .record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            Timer.builder("cassandraProxy.cqlOperation.timer")
                    .tag("requestOpcode", String.valueOf(opcode))
                    .tag("requestState", state.toString())
                    .register(registry)
                    .record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            Counter.builder("cassandraProxy.cqlOperation.cqlServerErrorCount")
                    .tag("requestOpcode", String.valueOf(opcode))
                    .tag("requestState", state.toString())
                    .register(registry).increment();
        }
        return serverCodec.encode(f).buffer;
    }

    private boolean isProtocolSupported(int protocolVersion) {
        for (Object o : commandLine.getOptionValues(PROTOCOL_VERSION)) {
            if (o.equals(protocolVersion)) {
                return true;
            }
        }
        return false;
    }

    private void writeToClientSocket(io.vertx.core.net.NetSocket socket, ProxyClient client1, ProxyClient client2, Buffer buf) {
        socket.write(buf);
        if (socket.writeQueueFull()) {
            LOG.warn("Pausing processing");
            client1.pause();
            client2.pause();
            final long startPause = System.nanoTime();
            socket.drainHandler(done -> {
                LOG.warn("Resuming processing");
                client1.resume();
                client2.resume();
                if ((Boolean)commandLine.getOptionValue("metrics")) {
                    MeterRegistry registry = BackendRegistries.getDefaultNow();
                    Timer.builder("cassandraProxy.clientSocket.paused")
                            .tag("clientAddress", socket.remoteAddress().toString())
                            .tag("wait", commandLine.getOptionValue("wait").toString())
                            .register(registry)
                            .record(System.nanoTime() - startPause, TimeUnit.NANOSECONDS);
                }
            });
        }
    }

    private void sendMetrics(long startTime, int opcode, FastDecode.State state, long endTime, Future<Buffer> f1, Future<Buffer> f2, Buffer buf) {
        MeterRegistry registry = BackendRegistries.getDefaultNow();
        if (FastDecode.quickLook(buf) == FastDecode.State.error) {
            Counter.builder("cassandraProxy.cqlOperation.cqlServerErrorCount")
                    .tag("requestOpcode", String.valueOf(opcode))
                    .tag("requestState", state.toString())
                    .register(registry).increment();
        }

        // Ignore prepared socne we handle that elsewhere and create a substitution, no need to count that
        // against us.
        if (state != FastDecode.State.prepare
                && !FastDecode.getMessage(f1.result(), ((Boolean)commandLine.getOptionValue("only-message"))).equals(FastDecode.getMessage(f2.result(), (Boolean)commandLine.getOptionValue("only-message")))) {
            try {
                // Turns out some implementations encode the result differentlty so we need to parse to be sure
                Frame f = clientCodec.decode(BufferCodec.createPrimitiveBuffer(f1.result()));
                Message m1 = f.message;
                Frame ff = clientCodec.decode(BufferCodec.createPrimitiveBuffer(f2.result()));
                Message m2 = ff.message;
                // .equals is often using Object.equals which is no good
                //if (!m1.toString().equals(m2.toString())) {
                Counter.builder("cassandraProxy.cqlOperation.cqlDifferentResultCount")
                        .tag("requestOpcode", String.valueOf(opcode))
                        .tag("requestState", state.toString())
                        .register(registry).increment();
                LOG.info("Different result");
                LOG.debug("Recieved cassandra server source: {} ", m1);
                LOG.debug("Raw: {}", FastDecode.getMessage(f1.result(), (Boolean) commandLine.getOptionValue("only-message")));
                LOG.debug("Recieved cassandra server destination: {} ", m2);
                LOG.debug("Raw: {}", FastDecode.getMessage(f2.result(), (Boolean) commandLine.getOptionValue("only-message")));
            } catch (Exception e) {
                LOG.warn("Exception decoding message: ", e);
            }
        }
        Timer.builder("cassandraProxy.cqlOperation.proxyTime")
                .tag("requestOpcode", String.valueOf(opcode))
                .tag("requestState", state.toString()).register(registry)
                .record(endTime - startTime, TimeUnit.NANOSECONDS);
        Timer.builder("cassandraProxy.cqlOperation.timer")
                .tag("requestOpcode", String.valueOf(opcode))
                .tag("requestState", state.toString())
                .register(registry)
                .record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    }

    protected Buffer handleUUID(Buffer buffer) {
        BufferCodec.PrimitiveBuffer buffer2 = BufferCodec.createPrimitiveBuffer(buffer);
        try {
            Frame f = serverCodec.decode(buffer2);
            LOG.info("Recieved: {}",  f.message);
            Message newMessage = f.message;
            if (f.message instanceof Query) {
                Query q = (Query) f.message;
                // Ideally we would be more targeted in replacing especially for
                // UPDATE and just target the SET part or the VALUES part
                // BATCH at least run by cqlsh will also come in as Query and not Batch type
                // so we handle this here as well.
                if (q.query.toUpperCase().startsWith("INSERT")
                        || q.query.toUpperCase().startsWith("UPDATE")
                        || (q.query.toUpperCase().startsWith("BEGIN BATCH") && (
                        q.query.toUpperCase().contains("INSERT") || q.query.toUpperCase().contains("UPDATE")))) {
                    String s = getReplacedQuery(q.query, UUID);
                    s = getReplacedQuery(s, NOW);
                    newMessage = new Query(s, q.options);
                }
            } else if (f.message instanceof Batch) {
                // Untested...
                Batch b = (Batch) f.message;
                List<Object> queriesOrIds = new ArrayList<>();
                for (Object o : b.queriesOrIds) {
                    if (o instanceof String) {
                        // it's a query and not just an id
                        String s = getReplacedQuery((String) o, UUID);
                        o = getReplacedQuery(s, NOW);
                    }
                    queriesOrIds.add(o);
                }
                List<List<ByteBuffer>> values = new ArrayList<>();
                for (List<ByteBuffer> list : b.values) {
                    List<ByteBuffer> v = new ArrayList<>();
                    for (ByteBuffer bb : list) {
                        String s = bb.toString();
                        if (s.trim().equalsIgnoreCase(UUID) || s.trim().equalsIgnoreCase(NOW)) {
                            ByteBuffer newBB = ByteBuffer.wrap(uuidGenWrapper.getTimeUUID().toString().getBytes());
                            v.add(newBB);
                            LOG.info("replaced {}  with {}", s, newBB);
                        } else {
                            v.add(bb);
                        }
                    }
                    values.add(v);
                }
                newMessage = new Batch(b.type, queriesOrIds, values, b.consistency, b.serialConsistency, b.defaultTimestamp, b.keyspace, b.nowInSeconds);
            }
            //  TODO: transform out prepared statement
            LOG.info("Replaced: {}", newMessage);
            Frame g = Frame.forRequest(f.protocolVersion, f.streamId, f.tracing, f.customPayload, newMessage);
            buffer = clientCodec.encode(g).buffer;
        } catch (Exception e) {
            LOG.error("Exception during decoding: ", e);
        }
        return buffer;
    }

    protected String getReplacedQuery(String q, String search) {
        int i = q.toUpperCase().indexOf(search);
        int j = 0;
        StringBuilder sb = new StringBuilder();
        while (i != -1) {
            sb.append(q.substring(j, i));
            j = i + search.length();
            sb.append(uuidGenWrapper.getTimeUUID());
            i = q.toUpperCase().indexOf(UUID, j);
        }
        sb.append(q.substring(j));
        return sb.toString();
    }

    private boolean scanForUUID(Buffer buffer) {
        String s = buffer.getString(9, buffer.length());
        return s.toUpperCase().contains(UUID) || s.toUpperCase().contains(NOW);
    }

    private boolean scanForPeers(Buffer buffer) {
        String s = buffer.getString(9, buffer.length());
        return s.toUpperCase().contains(PEERS);
    }

    private boolean scanForPeersV2(Buffer buffer) {
        String s = buffer.getString(9, buffer.length());
        return s.toUpperCase().contains(PEERS_V2);
    }

}
