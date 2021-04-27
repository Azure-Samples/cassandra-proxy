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

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.cli.*;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.PemKeyCertOptions;

import java.util.*;
import java.util.logging.*;


/*-h localhost localhost --proxy-pem-keyfile /home/german/Project/cassandra-proxy/src/main/resources/server.pem --proxy-pem-certfile /home/german/Project/cassandra-proxy/src/main/resources/server.key*/
public class Proxy extends AbstractVerticle {
    private static final Logger LOG = Logger.getLogger(Proxy.class.getName());
    private static final String CASSANDRA_SERVER_PORT = "29042";
    private static CommandLine commandLine;
    private BufferCodec bufferCodec = new BufferCodec();
    FrameCodec<BufferCodec.PrimitiveBuffer> serverCodec = FrameCodec.defaultServer(bufferCodec, Compressor.none());
    FrameCodec<BufferCodec.PrimitiveBuffer> clientCodec = FrameCodec.defaultClient(bufferCodec, Compressor.none());

    public static void main(String[] args)
    {
        CLI cli = CLI.create("cassandra-proxy")
                .setSummary("A dual write proxy for cassandra.")
                .addOption(
                        new Option().setLongName("help").setShortName("h").setFlag(true).setHelp(true))
                .addOption(new TypedOption<Boolean>()
                        .setType(Boolean.class)
                        .setLongName("wait")
                        .setShortName("W")
                        .setDescription("wait for write completed on both clusters")
                        .setFlag(true))
                .addArgument(new Argument()
                        .setDescription("Source cluster. This is the cluster which is authorative for reads")
                        .setRequired(true)
                        .setArgName("source"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Source cluster port. This is the cluster which is authorative for reads")
                        .setLongName("source-port")
                        .setDefaultValue("9042"))
                .addArgument(new Argument()
                        .setRequired(true)
                        .setDescription("Destination cluster. This is the cluster we ignore reads for.")
                        .setArgName("target"))
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("Destination cluster port. This is the cluster we ignore reads for.")
                        .setLongName("target-port")
                        .setDefaultValue("9042"))
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
                .addOption(new TypedOption<Integer>()
                        .setType(Integer.class)
                        .setDescription("How many threads should be launched")
                        .setLongName("threads")
                        .setShortName("t")
                        .setDefaultValue("1"))
                .addOption(new TypedOption<Integer>()
                    .setType(Integer.class)
                    .setDescription("Supported CQL Version(s). If not set return what source sever says")
                    .setLongName("cql-version")
                    .setMultiValued(true));
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

        LOG.info("Cassandra Proxy starting...");
        DeploymentOptions options = new DeploymentOptions().setInstances(commandLine.getOptionValue("threads"));
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new Proxy());
    }

    private static void help(CLI cli) {
        StringBuilder builder = new StringBuilder();
        cli.usage(builder);
        System.out.print(builder.toString());
    }

    @Override
    public void start() throws Exception {

        NetServerOptions options = new NetServerOptions().setPort(commandLine.getOptionValue("proxy-port"));
        if (commandLine.getOptionValue("proxy-pem-keyfile") != null && commandLine.getOptionValue("proxy-pem-certfile") != null) {
            PemKeyCertOptions pemOptions = new PemKeyCertOptions();
            pemOptions.addCertPath(commandLine.getOptionValue("proxy-pem-keyfile"))
                    .addKeyPath(commandLine.getOptionValue("proxy-pem-certfile"));
            options.setSsl(true).setPemKeyCertOptions(pemOptions);
        } else if (commandLine.getOptionValue("proxy-pem-keyfile") != null || commandLine.getOptionValue("proxy-pem-certfile") != null) {
            System.out.println("Both proxy-pem-keyfile and proxy-pem-certfile need to be set for TLS");
            System.exit(-1);
        }

        NetServer server = vertx.createNetServer(options);

        server.connectHandler(socket -> {
            ProxyClient client1 = new ProxyClient("client1", socket);
            Future c1 = client1.start(vertx, commandLine.getArgumentValue("source"), commandLine.getOptionValue("source-port"));
            ProxyClient client2 = new ProxyClient("client2", null);
            Future c2 = client2.start(vertx, commandLine.getArgumentValue("target"), commandLine.getOptionValue("target-port"));
            LOG.info("Both server up)");
            FastDecode fastDecode = FastDecode.newFixed(socket, buffer -> {
                FastDecode.State state = FastDecode.quickLook(buffer);
                // Check if we support the protocol version
                if (commandLine.getOptionValues("cql-version")!= null) {
                    int protocolVersion = buffer.getByte(0) & 0b0111_1111;
                    boolean found = false;
                    for (Object o : commandLine.getOptionValues("cql-version")) {
                        if (o.equals(protocolVersion))
                        {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        LOG.severe("Todo: Implement error when wrong protocol");
                        return;
                    }

                }
                // TODO: Scan for uuid() inserts and replace UUID as needed
//                if (state==FastDecode.State.analyze || state==FastDecode.State.query) {
//                    BufferCodec.PrimitiveBuffer buffer2 = BufferCodec.createPrimitiveBuffer(buffer);
//                    try {
//                        Frame f = serverCodec.decode(buffer2);
//                        LOG.info("Recieved: " + f.message);
//                        if (f.message instanceof Query) {
//                            Query q = (Query)f.message;
//                            if (q.query.equals("SELECT * FROM system.peers"))
//                            {
//                                LOG.fine("Peers!");
//                            }
//                        }
//                    } catch (Exception e) {
//                        LOG.severe("Eception during decoding: " + e);
//                    }
//                }
                Future<Buffer> f1 = client1.writeToServer(buffer).future();
                Future<Buffer> f2 = client2.writeToServer(buffer).future();
                CompositeFuture.all(f1, f2).onComplete(e -> {
                    if ((commandLine.getOptionValues("cql-version")!= null) && (state == FastDecode.State.options))
                    {
                        // Todo force our version
                        LOG.severe("Todo: Implement response with our favorite versions");
                        Buffer buf = f1.result();
                    }
                    Buffer buf = f1.result();
                    if (!f1.result().equals(f2.result())) {
                        LOG.info("Diferent result");
                        LOG.fine("Recieved cassandra server 1: " + f1.result());
                        LOG.fine("Recieved cassandra server 2: " + f2.result());
                    }
                    socket.write(buf);
                    if (socket.writeQueueFull()) {
                        LOG.warning("Pausing processing");
                        client1.fastDecode.pause();
                        client2.fastDecode.pause();
                        socket.drainHandler(done -> {
                            LOG.warning("Resuming processing");
                            client1.fastDecode.resume();
                            client2.fastDecode.resume();
                       });
                    }
                });
            });
            fastDecode.endHandler(x->{LOG.info("Connection closed!");});

        }).listen(res -> {
            if (res.succeeded()) {
                LOG.info("Server is now listening on  port: " + server.actualPort());
            } else {
                LOG.severe("Failed to bind!");
                System.exit(-1);
            }
        });

    }
}
