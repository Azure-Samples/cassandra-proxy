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
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import io.vertx.core.buffer.Buffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Credential {

    private String username;
    private String password;

    public Credential(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public AuthResponse replaceAuthentication(AuthResponse authResponse) {
        Buffer b = Buffer.buffer();
        //b.appendString(""); // authorizationId ?
        b.appendByte((byte) 0);
        b.appendString(username);
        b.appendByte((byte) 0);
        b.appendString(password);
        return new AuthResponse(ByteBuffer.wrap(b.getBytes()));
    }
}


