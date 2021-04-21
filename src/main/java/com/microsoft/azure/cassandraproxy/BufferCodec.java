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

import com.datastax.oss.protocol.internal.PrimitiveCodec;
import io.vertx.core.buffer.Buffer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.CRC32;

public class BufferCodec implements PrimitiveCodec<BufferCodec.PrimitiveBuffer> {

    public static PrimitiveBuffer createPrimitiveBuffer(Buffer b)
    {
        return new PrimitiveBuffer(b);
    }

    @Override
    public PrimitiveBuffer allocate(int size) {
        return new PrimitiveBuffer(size);
    }

    @Override
    public void release(PrimitiveBuffer toRelease) {
        // noop
    }

    @Override
    public int sizeOf(PrimitiveBuffer toMeasure) {
        return toMeasure.buffer.length();
    }

    @Override
    public PrimitiveBuffer concat(PrimitiveBuffer left, PrimitiveBuffer right) {
        left.buffer.appendBuffer(right.buffer);
        return left;
    }

    @Override
    public void markReaderIndex(PrimitiveBuffer source) {
        // no-op
    }

    @Override
    public void resetReaderIndex(PrimitiveBuffer source) {
        source.readPosition = 0;
    }

    @Override
    public byte readByte(PrimitiveBuffer source) {
        return source.buffer.getByte(source.readPosition++);
    }

    @Override
    public int readInt(PrimitiveBuffer source) {
        int i = source.buffer.getInt(source.readPosition);
        source.readPosition += Integer.BYTES;
        return i;
    }

    @Override
    public int readInt(PrimitiveBuffer source, int offset) {
        // not sure if we need to multiply the offset?
        return source.buffer.getInt(source.readPosition + offset);
    }

    @Override
    public InetAddress readInetAddr(PrimitiveBuffer source) {
        int length = readByte(source) & 0xFF;
        byte[] bytes = new byte[length];
        source.readBytes(bytes);
        return newInetAddress(bytes);
    }

    @Override
    public long readLong(PrimitiveBuffer source) {
        long l = source.buffer.getLong(source.readPosition);
        source.readPosition += Long.BYTES;
        return l;
    }

    @Override
    public int readUnsignedShort(PrimitiveBuffer source) {
        int uShort = source.buffer.getUnsignedShort(source.readPosition);
        source.readPosition += Short.BYTES;
        return uShort;
    }

    @Override
    public ByteBuffer readBytes(PrimitiveBuffer source) {
        int length = readInt(source);
        if (length < 0)
        {
            return null;
        }
        return ByteBuffer.wrap(source.readBytes(new byte[length]));
    }

    @Override
    public byte[] readShortBytes(PrimitiveBuffer source) {
        try {
            int length = readUnsignedShort(source);
            byte[] bytes = new byte[length];
            return source.readBytes(bytes);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read a byte array preceded by its 2 bytes length");
        }
    }

    @Override
    public String readString(PrimitiveBuffer source) {
        int length = readUnsignedShort(source);
        return readString(source, length);
    }

    @Override
    public String readLongString(PrimitiveBuffer source) {
        int length = readInt(source);
        return readString(source, length);
    }

    @Override
    public PrimitiveBuffer readRetainedSlice(PrimitiveBuffer source, int sliceLength) {
        return new PrimitiveBuffer(source.buffer.slice(source.readPosition, source.readPosition + sliceLength));
    }

    @Override
    public void updateCrc(PrimitiveBuffer source, CRC32 crc) {
        crc.update(source.buffer.getBytes());
    }

    @Override
    public void writeByte(byte b, PrimitiveBuffer dest) {
        dest.buffer.appendByte(b);
    }

    @Override
    public void writeInt(int i, PrimitiveBuffer dest) {
        dest.buffer.appendInt(i);
    }

    @Override
    public void writeInetAddr(InetAddress address, PrimitiveBuffer dest) {
        byte[] bytes = address.getAddress();
        writeByte((byte) bytes.length, dest);
        dest.buffer.appendBytes(bytes);
    }

    @Override
    public void writeLong(long l, PrimitiveBuffer dest) {
        dest.buffer.appendLong(l);
    }

    @Override
    public void writeUnsignedShort(int i, PrimitiveBuffer dest) {
        dest.buffer.appendUnsignedShort(i);
    }

    @Override
    public void writeString(String s, PrimitiveBuffer dest) {
        writeUnsignedShort(s.length(), dest);
        dest.buffer.appendString(s);
    }

    @Override
    public void writeLongString(String s, PrimitiveBuffer dest) {
        writeInt(s.length(), dest);
        dest.buffer.appendString(s);
    }

    @Override
    public void writeBytes(ByteBuffer bytes, PrimitiveBuffer dest) {
        if (bytes == null) {
            writeInt(-1, dest);
        } else {
            writeInt(bytes.remaining(), dest);
            dest.buffer.appendBytes(bytes.array());
        }
    }

    @Override
    public void writeBytes(byte[] bytes, PrimitiveBuffer dest) {
        if (bytes == null) {
            writeInt(-1, dest);
        } else {
            writeInt(bytes.length, dest);
            dest.buffer.appendBytes(bytes);
        }
    }

    @Override
    public void writeShortBytes(byte[] bytes, PrimitiveBuffer dest) {
        writeUnsignedShort(bytes.length, dest);
        dest.buffer.appendBytes(bytes);
    }

    private static String readString(PrimitiveBuffer source, int length) {
        try {
            String str = source.buffer.getString(source.readPosition, source.readPosition + length);
            source.readPosition += length;
            return str;
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                    "Not enough bytes to read an UTF-8 serialized string of size " + length, e);
        }
    }


    private InetAddress newInetAddress(byte[] bytes) {
        try {
            return InetAddress.getByAddress(bytes);
        } catch (UnknownHostException e) {
            // Per the Javadoc, the only way this can happen is if the length is illegal
            throw new IllegalArgumentException(
                    String.format("Invalid address length: %d (%s)", bytes.length, Arrays.toString(bytes)));
        }
    }

    public static class PrimitiveBuffer
    {
        protected int readPosition = 0;
        protected Buffer buffer;

        public PrimitiveBuffer(int size)
        {
            this(Buffer.buffer(size));
        }

        public PrimitiveBuffer(Buffer buffer)
        {
            this.buffer = buffer;
        }

        public byte[] readBytes(byte[] bytes)
        {
            buffer.getBytes(this.readPosition, this.readPosition + bytes.length, bytes);
            this.readPosition += bytes.length;
            return bytes;
        }

        public boolean isEof()
        {
            return this.buffer.length() <= this.readPosition;
        }
    }
}
