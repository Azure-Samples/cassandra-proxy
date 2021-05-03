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

import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.Flags;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import java.util.Objects;
import io.netty.buffer.Unpooled;

/**
 * Divides the stream into Cassandra frames for later processing
 * Waits until a frame is complete
 * Has a static function to analyze the opcode into some state to avoid
 * full parsing
 */
public class FastDecode implements ReadStream<Buffer>, Handler<Buffer>
{
    private static final Buffer EMPTY_BUFFER = Buffer.buffer(Unpooled.EMPTY_BUFFER);
    private Buffer buff = EMPTY_BUFFER;
    private int pos;            // Current position in buffer
    private int start;          // Position of beginning of current record

    private int recordSize;
    private long demand = Long.MAX_VALUE;
    private Handler<Buffer> eventHandler;
    private Handler<Void> endHandler;
    private Handler<Throwable> exceptionHandler;
    private boolean parsing;
    private boolean streamEnded;
    private boolean header = true;

    private final ReadStream<Buffer> stream;

    private FastDecode(ReadStream<Buffer> stream) {
        this.stream = stream;
    }

    public FastDecode exceptionHandler(Handler<Throwable> handler) {
        exceptionHandler = handler;
        return this;
    }

    public FastDecode pause() {
        demand = 0L;
        return this;
    }


    public FastDecode resume() {
        return fetch(Long.MAX_VALUE);
    }

    public FastDecode fetch(long amount) {
        demand += amount;
        if (demand < 0L) {
            demand = Long.MAX_VALUE;
        }
        handleParsing();
        return this;
    }
    private void end() {
        Handler<Void> handler = endHandler;
        if (handler != null) {
            handler.handle(null);
        }
    }

    public void setOutput(Handler<Buffer> output) {
        Objects.requireNonNull(output, "output");
        eventHandler = output;
    }

    public FastDecode handler(Handler<Buffer> handler) {
        eventHandler = handler;
        if (stream != null) {
            if (handler != null) {
                stream.endHandler(v -> {
                    streamEnded = true;
                    handleParsing();
                });
                stream.exceptionHandler(err -> {
                    if (exceptionHandler != null) {
                        exceptionHandler.handle(err);
                    }
                });
                stream.handler(this);
            } else {
                stream.handler(null);
                stream.endHandler(null);
                stream.exceptionHandler(null);
            }
        }
        return this;
    }


    public static FastDecode newFixed(ReadStream<Buffer> stream, Handler<Buffer> output) {
        FastDecode ls = new FastDecode(stream);
        ls.handler(output);
        return ls;
    }

    public void handle(Buffer buffer) {
        if (buff.length() == 0) {
            buff = buffer;
        } else {
            buff.appendBuffer(buffer);
        }
        handleParsing();
    }

    private void handleParsing() {
        if (parsing) {
            return;
        }
        parsing = true;
        try {
            do {
                if (demand > 0L) {
                    int next = parseFixed();

                    if (next == -1) {
                        if (streamEnded) {
                            if (buff.length() == 0) {
                                break;
                            }
                            next = buff.length();
                        } else {
                            ReadStream<Buffer> s = stream;
                            if (s != null) {
                                s.resume();
                            }
                            if (streamEnded) {
                                continue;
                            }
                            break;
                        }
                    }
                    if (demand != Long.MAX_VALUE) {
                        demand--;
                    }
                    Buffer event = buff.getBuffer(start, next);
                    start = pos;
                    Handler<Buffer> handler = eventHandler;
                    if (handler != null) {
                        handler.handle(event);
                    }
                    if (streamEnded) {
                        break;
                    }
                } else {
                    // Should use a threshold ?
                    ReadStream<Buffer> s = stream;
                    if (s != null) {
                        s.pause();
                    }
                    break;
                }
            } while (true);
            int len = buff.length();
            if (start == len) {
                buff = EMPTY_BUFFER;
            } else if (start > 0) {
                buff = buff.getBuffer(start, len);
            }
            pos -= start;
            start = 0;
            if (streamEnded) {
                end();
            }
        } finally {
            parsing = false;
        }
    }

    private int parseFixed() {
        int len = buff.length();
        if (header) {
            if (len - start >= 9) {
                recordSize = buff.getInt(start + 5) + 9;
                header = false;
            } else {
                return -1;
            }
        }
        if (len - start >= recordSize) {
            int end = start + recordSize;
            pos = end;
            header = true;
            return end;
        }
        return -1;
    }

    public enum State {
        query, analyze, result, prepare, error, event, supported;
    }

    public static State quickLook(Buffer buffer)
    {
        int opcode =  buffer.getByte(4);

        switch (opcode)
        {
            case ProtocolConstants.Opcode.QUERY:
            case ProtocolConstants.Opcode.BATCH:
            case ProtocolConstants.Opcode.EXECUTE:
                return State.query;
            case ProtocolConstants.Opcode.PREPARE:
                return State.prepare;
            case ProtocolConstants.Opcode.ERROR:
                return State.error;
            case ProtocolConstants.Opcode.EVENT:
                return State.event;
            case ProtocolConstants.Opcode.RESULT:
                return State.result;
            case ProtocolConstants.Opcode.SUPPORTED:
                return State.supported;
        }
        return State.analyze;
    }

    public FastDecode endHandler(Handler<Void> handler) {
        endHandler = handler;
        return this;
    }
}
