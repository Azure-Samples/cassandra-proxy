package com.microsoft.azure.cassandraproxy;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import io.vertx.core.buffer.Buffer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.UUID;

@RunWith(MockitoJUnitRunner.class)
public class ProxyTest {

    private Proxy proxy;
    private UUIDGenWrapper mockUuidGen;
    private FrameCodec<BufferCodec.PrimitiveBuffer> mockServerCodec;
    private FrameCodec<BufferCodec.PrimitiveBuffer> mockClientCodec;


    @Before
    public void before() {
        mockUuidGen = mock(UUIDGenWrapper.class);
        mockServerCodec = mock(FrameCodec.class);
        mockClientCodec = mock(FrameCodec.class);
        proxy = new Proxy(mockUuidGen, mockServerCodec, mockClientCodec);

    }

    @Test
    public void testGetReplacedQueryOnce() {
        UUID uuid = UUID.randomUUID();
        when(mockUuidGen.getTimeUUID()).thenReturn(uuid);
        assertEquals("INSERT INTO cycling.cyclist_name (id, lastname, firstname)\n" +
                "  VALUES (" + uuid + ", 'KRUIKSWIJK','Steven')\n" +
                "  USING TTL 86400 AND TIMESTAMP 123456789;",
                proxy.getReplacedQuery("INSERT INTO cycling.cyclist_name (id, lastname, firstname)\n" +
                        "  VALUES (" + Proxy.UUID + ", 'KRUIKSWIJK','Steven')\n" +
                        "  USING TTL 86400 AND TIMESTAMP 123456789;",
                        Proxy.UUID)
                );
    }

    @Test
    public void testGetReplacedQueryTwice() {
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        when(mockUuidGen.getTimeUUID()).thenReturn(uuid1, uuid2);
        assertEquals("INSERT INTO cycling.cyclist_name (id, lastname, firstname)\n" +
                        "  VALUES (" + uuid1 + ", 'KRUIKSWIJK'," + uuid2 + ")\n" +
                        "  USING TTL 86400 AND TIMESTAMP 123456789;",
                proxy.getReplacedQuery("INSERT INTO cycling.cyclist_name (id, lastname, firstname)\n" +
                                "  VALUES (" + Proxy.UUID + ", 'KRUIKSWIJK'," + Proxy.UUID +")\n" +
                                "  USING TTL 86400 AND TIMESTAMP 123456789;",
                        Proxy.UUID)
        );
    }

    @Test
    public void testHandleUUIDNoQueryMatch()
    {
        Query q = new Query("test", null);
        Frame f = Frame.forRequest(0,0,false, Collections.emptyMap(),q);
        when(mockServerCodec.decode(any())).thenReturn(f);
        ArgumentCaptor<Frame> argumentCaptor = ArgumentCaptor.forClass(Frame.class);
        proxy.handleUUID(Buffer.buffer());
        verify(mockClientCodec).encode(argumentCaptor.capture());
        Frame g = argumentCaptor.getValue();
        assertEquals(f.message.toString(), g.message.toString());
    }

    @Test
    public void testHandleUUIDNQueryMatch()
    {
        UUID uuid1 = UUID.randomUUID();
        when(mockUuidGen.getTimeUUID()).thenReturn(uuid1);
        Query q = new Query("BEGIN BATCH INSERT INTO cycling.cyclist_expenses (cyclist_name, balance) VALUES ('Vera ADRIAN', 0) IF NOT EXISTS;   INSERT INTO cycling.cyclist_expenses (cyclist_name, expense_id, amount, description, paid) VALUES (UUID(), 1, 7.95, 'Breakfast', false);   APPLY BATCH;", null);
        Frame f = Frame.forRequest(0,0,false, Collections.emptyMap(),q);
        when(mockServerCodec.decode(any())).thenReturn(f);
        ArgumentCaptor<Frame> argumentCaptor = ArgumentCaptor.forClass(Frame.class);
        proxy.handleUUID(Buffer.buffer());
        verify(mockClientCodec).encode(argumentCaptor.capture());
        Frame g = argumentCaptor.getValue();
        assertEquals("BEGIN BATCH INSERT INTO cycling.cyclist_expenses (cyclist_name, balance) VALUES ('Vera ADRIAN', 0) IF NOT EXISTS;   INSERT INTO cycling.cyclist_expenses (cyclist_name, expense_id, amount, description, paid) VALUES (" + uuid1 +", 1, 7.95, 'Breakfast', false);   APPLY BATCH;", ((Query)g.message).query);
    }

    @Test
    public void testCheckUnprepared()
    {
        assertFalse(proxy.checkUnpreparedTarget(FastDecode.State.error, null));

        Error error = new Unprepared("Boo", new byte[]{0});
        Frame f = Frame.forResponse(3, 0, null, Collections.emptyMap(), Collections.emptyList(), error);
        when(mockClientCodec.decode(any())).thenReturn(f);
        FrameCodec<BufferCodec.PrimitiveBuffer> serverCodec = FrameCodec.defaultServer(new BufferCodec(), Compressor.none());
        assertTrue(proxy.checkUnpreparedTarget(FastDecode.State.query, serverCodec.encode(f).buffer));
    }


}