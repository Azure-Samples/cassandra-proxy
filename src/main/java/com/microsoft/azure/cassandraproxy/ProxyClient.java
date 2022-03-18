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
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.NetClientOptions;
import io.vertx.micrometer.backends.BackendRegistries;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyClient  {
    private static final Logger LOG = LoggerFactory.getLogger(ProxyClient.class);
    private static final BufferCodec bufferCodec = new BufferCodec();
    private static final FrameCodec<BufferCodec.PrimitiveBuffer> serverCodec = FrameCodec.defaultServer(bufferCodec, Compressor.none());
    private static final FrameCodec<BufferCodec.PrimitiveBuffer> clientCodec = FrameCodec.defaultClient(bufferCodec, Compressor.none());
    private final String identifier;
    private final List<String> protocolVersions;
    private final List<String> cqlVersions;
    private final List<String> compressions;
    private final boolean compressionEnabled;
    private final boolean metrics;
    private final boolean wait;
    private final Credential credential;
    private NetSocket socket;
    private Promise<Void> socketPromise;
    private Promise<Buffer> bufferPromise = Promise.promise();
    private final NetSocket serverSocket;
    private FastDecode fastDecode;
    // Map to hold the requests so we can assign out of order responses
    // to the right request Promise
    private Map<Short, Promise> results = new ConcurrentHashMap<>();
    // Prepare statements are identified by the md5 hash of the query string
    // Some implementations calculate md5 differently and so we need to keep track of them
    // Prepared statements are only cached for the duration of a session (@TODO: Confirm)
    // but we will cache them for the life of the proxy to save memory
    private static Map<String, byte[]> prepareSubstitution = new ConcurrentHashMap<>();
    private boolean closed = false;
    // for metrics
    private Map<Short, Long> startTime = new ConcurrentHashMap<>();
    private Map<Short, Long> endTime = new ConcurrentHashMap<>();



    public ProxyClient(String identifier, NetSocket socket, List<String> protocolVersions, List<String> cqlVersions, List<String> compressions, boolean compressionEnabled, boolean metrics, boolean wait, Credential credential) {
        this.identifier = identifier;
        this.serverSocket = socket;
        this.protocolVersions = protocolVersions;
        this.cqlVersions = cqlVersions;
        this.compressions = compressions;
        this.metrics = metrics;
        this.wait = wait;
        this.compressionEnabled = compressionEnabled;
        this.credential = credential;
    }

    public ProxyClient(String identifier, boolean metrics, Credential credential) {
        this(identifier, null, null, null, null, true, metrics, true, credential);
    }

    public void pause() {
        fastDecode.pause();
    }

    public void resume() {
        fastDecode.resume();
    }

    public Future<Void> start(Vertx vertx,String host, int port, boolean ssl, int timeout)
    {
        socketPromise = Promise.promise();
        // @TODO: Allow for truststore, etc,
        NetClientOptions options = new NetClientOptions();
        if (ssl) {
            options.setSsl(true).setTrustAll(true);
        }
        options.setIdleTimeout(timeout);
        options.setIdleTimeoutUnit(TimeUnit.SECONDS);
        vertx.createNetClient(options).connect(port, host, res-> {
            if (res.succeeded()) {
                LOG.info("Server connected");
                socket = res.result();
                fastDecode = FastDecode.newFixed(socket, b-> clientHandle(b));
                fastDecode.endHandler(x->{
                    this.closed = true;
                    LOG.info("Server connection closed");
                });
                socketPromise.complete();
            }  else {
                this.closed = true;
                LOG.error(String.format("%s - Couldn't connect to server", this.identifier));
                socketPromise.fail("Couldn't connect to server");
            }
        });
        return socketPromise.future();
    }

    public Promise<Buffer> writeToServer(Buffer buffer) {
        bufferPromise = Promise.promise();
        short streamId = buffer.getShort(2);
        results.put(streamId, bufferPromise);
        startTime.put(streamId, System.nanoTime());
        if (socketPromise != null) {
            socketPromise.future().onSuccess(t -> {
                write(buffer);
            });
        } else {
           write(buffer);
        }

        return bufferPromise;
    }

    private void write(Buffer buffer) {
        // Do we need to substitute the queryId ?
        if ((serverSocket == null) && (buffer.getByte(4) == ProtocolConstants.Opcode.EXECUTE) && !this.prepareSubstitution.isEmpty()) {
            BufferCodec.PrimitiveBuffer buffer2 = BufferCodec.createPrimitiveBuffer(buffer);
            Frame f = serverCodec.decode(buffer2);
            Execute execute = (Execute) f.message;
            LOG.debug("Need to substitute prepared query id {}", execute.queryId);
            if (prepareSubstitution.containsKey(Bytes.toHexString(execute.queryId))) {
                Execute newExecute = new Execute(prepareSubstitution.get(Bytes.toHexString(execute.queryId)), execute.resultMetadataId, execute.options);
                LOG.debug("Substituting {} for {}", execute, newExecute);
                Frame r = Frame.forRequest(f.protocolVersion, f.streamId, f.tracing, f.customPayload, newExecute);
                buffer = clientCodec.encode(r).buffer;
            } else {
                LOG.debug("QueryId not found in {}", prepareSubstitution);
            }
        } else if ((buffer.getByte(4) == ProtocolConstants.Opcode.AUTH_RESPONSE) && credential !=null) {
            LOG.debug("Changing Auth");
            BufferCodec.PrimitiveBuffer buffer2 = BufferCodec.createPrimitiveBuffer(buffer);
            Frame f = serverCodec.decode(buffer2);
            Frame r = Frame.forRequest(f.protocolVersion, f.streamId, f.tracing, f.customPayload, credential.replaceAuthentication((AuthResponse) f.message));
            buffer = clientCodec.encode(r).buffer;

        }
        socket.write(buffer);
        if (socket.writeQueueFull()) {
            LOG.warn("{} Write Queue full!", identifier);
            final long startPause = System.nanoTime();
            if (serverSocket != null) {
                serverSocket.pause();
                socket.drainHandler(done -> {
                    LOG.warn("Resume processing");
                    serverSocket.resume();
                    if (metrics) {
                        MeterRegistry registry = BackendRegistries.getDefaultNow();
                        Timer.builder("cassandraProxy.serverSocket.paused")
                                .tag("serverAddress", socket.remoteAddress().toString())
                                .tag("severIdentifier", identifier)
                                .register(registry)
                                .record(System.nanoTime() - startPause, TimeUnit.NANOSECONDS);
                    }
                });
            }
        }
    }


    private void clientHandle(Buffer buffer)
    {
            FastDecode.State state = fastDecode.quickLook(buffer);
            // Handle Supported
            if ((serverSocket != null) && (state == FastDecode.State.supported) && (!protocolVersions.isEmpty() || !cqlVersions.isEmpty() || !compressions.isEmpty())) {
                BufferCodec.PrimitiveBuffer buffer2 = BufferCodec.createPrimitiveBuffer(buffer);
                Frame r = clientCodec.decode(buffer2);
                Supported supported = (Supported) r.message;
                LOG.info("Recieved from Server {} : {}", identifier, supported);
                // INFO: Recieved from Server client2:SUPPORTED {PROTOCOL_VERSIONS=[3/v3, 4/v4, 5/v5-beta], COMPRESSION=[snappy, lz4], CQL_VERSION=[3.4.4]}
                Map<String, List<String>> options = new HashMap<>(supported.options);
                if ((protocolVersions != null) && (!protocolVersions.isEmpty())) {
                    options.put("PROTOCOL_VERSIONS", protocolVersions);
                }
                if ((cqlVersions !=null) && (!cqlVersions.isEmpty())) {
                    options.put("CQL_VERSION", cqlVersions);
                }
                if ((compressions !=null) && (!compressions.isEmpty())) {
                    options.put("COMPRESSION", compressions);
                }
                if (!compressionEnabled) {
                    options.put("COMPRESSION", Collections.EMPTY_LIST);
                }
                supported = new Supported(options);
                LOG.info("Sending to Client {} : {}", identifier, supported);
                Frame f = Frame.forResponse(r.protocolVersion, r.streamId, r.tracingId, r.customPayload , r.warnings, supported);
                sendResult(serverCodec.encode(f).buffer);
                return;
            }

            // TODO: Do something for event
            if (state == FastDecode.State.event || state == FastDecode.State.error) {
                try {
                    BufferCodec.PrimitiveBuffer buffer2 = BufferCodec.createPrimitiveBuffer(buffer);
                    Frame r = clientCodec.decode(buffer2);
                    LOG.info("Recieved from Server {} : {}", identifier , r.message);
                    sendResult(buffer);
                } catch (Exception e) {
                    LOG.error("Failed decoding: ", e);
                    if (socketPromise.tryComplete()) {
                       sendResult(buffer);
                    }
                }
            } else {
               sendResult(buffer);
            }
    }

    private void sendResult(Buffer buffer) {
        if (!wait && socket != null) {
            socket.write(buffer);
            if (socket.writeQueueFull()) {
                LOG.warn("Pausing processing");
                this.pause();
                final long startPause = System.nanoTime();
                socket.drainHandler(done -> {
                    LOG.warn("Resuming processing");
                    this.resume();
                    if (metrics) {
                        MeterRegistry registry = BackendRegistries.getDefaultNow();
                        Timer.builder("cassandraProxy.clientSocket.paused")
                                .tag("clientAddress", socket.remoteAddress().toString())
                                .tag("wait", String.valueOf(wait)).register(registry)
                                .record(System.nanoTime() - startPause, TimeUnit.NANOSECONDS);
                    }
                });
            }
        }
        short streamId = buffer.getShort(2);
        if (results.containsKey(streamId)) {
            Promise promise = results.get(streamId);
            endTime.put(streamId, System.nanoTime());
            if (!promise.tryComplete(buffer)) {
                LOG.warn("out of band: {}", buffer);
            }
            results.remove(streamId); // we are done with that
        } else if (streamId != -1){ //-1 is EVENT
            LOG.warn ("Stream Id {} no registered. Are you using TLS on a non TLS connection?", streamId);
        }
    }

    public void addPrepareSubstitution(byte[] orig, byte[] target) {
        this.prepareSubstitution.put(Bytes.toHexString(orig), target);
    }

    public void close() {
        if (this.socket != null) {
            this.socket.end();
        }
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    public Long getStartTime(short streamId) {
        return startTime.remove(streamId);
    }

    public Long getEndTime(short streamId) {
        return endTime.remove(streamId);
    }
}
