/*
 * Copyright (C) 2013 Suresh Mahalingam.  All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE FOR
 *  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

package org.aredis.net;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.TimeUnit;

import org.aredis.cache.AsyncHandler;
import org.aredis.cache.AsyncRedisConnection;
import org.aredis.util.concurrent.SimpleThreadFactory;

/**
 * Java 7 NIO Channel API based default implementation of {@link AsyncSocketTransport}.
 * @author suresh
 *
 */
public class AsyncJavaSocketTransport extends AbstractAsyncSocketTransport {

    private AsynchronousSocketChannel channel;

    private ByteBuffer readBuffer;

    private ByteBuffer writeBuffer;

    private byte [] tempBuf = new byte[64];

    private CompletionHandler<Void, AsyncHandler<Boolean>> connectCompletionHandler;

    private ReadCompletionHandler readCompletionHandler;

    private WriteCompletionHandler writeCompletionHandler;

    private FlushCompletionHandler flushCompletionHandler;

    private class ConnectCompletionHandler implements CompletionHandler<Void, AsyncHandler<Boolean>> {

        @Override
        public void completed(Void result, AsyncHandler<Boolean> handler) {
            lastUseTime = System.currentTimeMillis();
            lastConnectTime = lastUseTime;
            updateConnectionStatus(ConnectionStatus.OK, null);
            handler.completed(true, null);
        }

        @Override
        public void failed(Throwable e, AsyncHandler<Boolean> handler) {
            updateConnectionStatus(ConnectionStatus.DOWN, e);
            handler.completed(false, e);
        }

    }

    private class ReadCompletionHandler implements CompletionHandler<Integer, AsyncHandler<Integer>> {

        private OutputStream dest;

        private int maxBytes;

        @Override
        public void completed(Integer result, AsyncHandler<Integer> handler) {
            if (result > 0) {
                lastUseTime = System.currentTimeMillis();
                readBuffer.flip();
                readInternal(dest, maxBytes, handler, true);
            } else {
                failed(new EOFException("EOF detected. Server Closed Connection."), handler);
            }
        }

        @Override
        public void failed(Throwable e, AsyncHandler<Integer> handler) {
            updateConnectionStatus(ConnectionStatus.DOWN, e);
            handler.completed(-1, e);
        }

    }

    private class WriteCompletionHandler implements CompletionHandler<Integer, AsyncHandler<Integer>> {

        private byte [] b;

        private int ofs;

        private int len;

        private int bytesTransferred;

        @Override
        public void completed(Integer result, AsyncHandler<Integer> handler) {
            lastUseTime = System.currentTimeMillis();
            writeBuffer.compact();
            bytesTransferred += writeBuffer.remaining();
            writeInternal(b, ofs, len, handler, true);
        }

        @Override
        public void failed(Throwable e, AsyncHandler<Integer> handler) {
            updateConnectionStatus(ConnectionStatus.DOWN, e);
            handler.completed(-1, e);
        }

    }

    private class FlushCompletionHandler implements CompletionHandler<Integer, AsyncHandler<Integer>> {

        @Override
        public void completed(Integer result, AsyncHandler<Integer> handler) {
            lastUseTime = System.currentTimeMillis();
            writeBuffer.compact();
            int res = result;
            if(!requiresFlush()) {
                res = - res;
            }
            handler.completed(res, null);
        }

        @Override
        public void failed(Throwable e, AsyncHandler<Integer> handler) {
            updateConnectionStatus(ConnectionStatus.DOWN, e);
            handler.completed(0, e);
        }

    }

    private static class SendFailedTask implements Runnable {

        private CompletionHandler<Integer, AsyncHandler<Integer>> completionHandler;

        private Throwable e;

        private AsyncHandler<Integer> handler;

        public SendFailedTask(CompletionHandler<Integer, AsyncHandler<Integer>> pcompletionHandler, Throwable pe, AsyncHandler<Integer> phandler) {
            completionHandler = pcompletionHandler;
            e = pe;
            handler = phandler;
        }

        @Override
        public void run() {
            completionHandler.failed(e, handler);
        }

    }

    /**
     * Channel group used by the AsyncSocketChannels. This can be changed before the 1st AsyncJavaSocketTransport is created.
     */
    public static AsynchronousChannelGroup channelGroup;

    public synchronized static AsynchronousChannelGroup getChannelGroup() throws IOException {
        if(channelGroup == null) {
            int initialSize = 10;
            channelGroup = AsynchronousChannelGroup.withFixedThreadPool(initialSize, SimpleThreadFactory.daemonThreadFactory);
        }

        return channelGroup;
    }

    /**
     * Sets a custom channel group. This should be called before an AsyncJavaSocketTransport object is created.
     * @param pchannelGroup Channel Group to use
     */
    public synchronized static void setChannelGroup(AsynchronousChannelGroup pchannelGroup) {
        channelGroup = pchannelGroup;
    }

    /**
     * Creates an AsyncJavaSocketTransport.
     * @param phost Server Host
     * @param pport Server Port
     * @param ptransportFactory TransportFactory to be associated with this transport
     * @throws IOException
     */
    public AsyncJavaSocketTransport(String phost, int pport, AsyncSocketTransportFactory ptransportFactory) throws IOException {
        super(phost, pport);
        connectCompletionHandler = new ConnectCompletionHandler();
        readCompletionHandler = new ReadCompletionHandler();
        writeCompletionHandler = new WriteCompletionHandler();
        flushCompletionHandler = new FlushCompletionHandler();
        transportFactory = ptransportFactory;
        config = transportFactory.getConfig();
        getChannelGroup();
    }

    @Override
    public void connectInternal(final AsyncHandler<Boolean> handler) {
        try {
            close();
            if(readBuffer == null || readBuffer.capacity() != config.getReadBufSize()) {
                readBuffer = ByteBuffer.allocate(config.getReadBufSize());
            }
            else {
                readBuffer.clear();
            }
            readBuffer.limit(0);
            if(writeBuffer == null || writeBuffer.capacity() != config.getWriteBufSize()) {
                writeBuffer = ByteBuffer.allocate(config.getWriteBufSize());
            }
            else {
                writeBuffer.clear();
            }
            InetSocketAddress address = new InetSocketAddress(host, port);
            AsynchronousChannelGroup channelGroup = getChannelGroup();
            channel = AsynchronousSocketChannel.open(channelGroup);
            channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            channel.connect(address, handler, connectCompletionHandler);
        } catch (final Exception e) {
            AsyncRedisConnection.bootstrapExecutor.execute(new Runnable() {

                @Override
                public void run() {
                    connectCompletionHandler.failed(e, handler);
                }

            });
        }
    }

    private int readInternal(OutputStream dest, int maxBytes, AsyncHandler<Integer> handler, boolean fromCallBack) {
        int bytesRead = 0;
        try {
            if(readBuffer.hasRemaining()) {
                int remaining = readBuffer.remaining();
                if(remaining > maxBytes) {
                    remaining = maxBytes;
                }
                do {
                    int len = tempBuf.length;
                    if(len > remaining) {
                        len = remaining;
                    }
                    readBuffer.get(tempBuf, 0, len);
                    dest.write(tempBuf, 0, len);
                    remaining -= len;
                } while(remaining > 0);
                bytesRead = maxBytes - remaining;
                if(fromCallBack) {
                    handler.completed(bytesRead, null);
                }
            }
            else {
                readBuffer.clear();
                readCompletionHandler.dest = dest;
                readCompletionHandler.maxBytes = maxBytes;
                channel.read(readBuffer, config.getReadTimeoutMillis(), TimeUnit.MILLISECONDS, handler, readCompletionHandler);
            }
        }
        catch(Exception e) {
            bytesRead = 0;
            AsyncRedisConnection.bootstrapExecutor.execute(new SendFailedTask(readCompletionHandler, e, handler));
        }

        return bytesRead;
    }

    @Override
    public int read(OutputStream dest, int maxBytes, AsyncHandler<Integer> handler) {
        return readInternal(dest, maxBytes, handler, false);
    }

    private int writeInternal(byte[] b, int ofs, int len, AsyncHandler<Integer> handler, boolean fromCallback) {
        int bytesWritten = writeBuffer.remaining();
        if(bytesWritten > len) {
            bytesWritten = len;
        }
        int retVal = bytesWritten;
        try {
            if(bytesWritten > 0) {
                writeBuffer.put(b, ofs, bytesWritten);
            }
            if(bytesWritten < len) {
                retVal = 0;
                if(!fromCallback) {
                    writeCompletionHandler.bytesTransferred = 0;
                }
                writeBuffer.flip();
                writeCompletionHandler.b = b;
                writeCompletionHandler.ofs = ofs + bytesWritten;
                writeCompletionHandler.len = len - bytesWritten;
                channel.write(writeBuffer, config.getWriteTimeoutMillis(), TimeUnit.MILLISECONDS, handler, writeCompletionHandler);
            }
            else if(fromCallback) {
                int bytesTransferred = writeCompletionHandler.bytesTransferred;
                writeCompletionHandler.bytesTransferred = 0;
                handler.completed(bytesTransferred, null);
            }
        }
        catch (Exception e) {
            retVal = 0;
            AsyncRedisConnection.bootstrapExecutor.execute(new SendFailedTask(writeCompletionHandler, e, handler));
        }

        return retVal;
    }

    @Override
    public int write(byte[] b, int ofs, int len, AsyncHandler<Integer> handler) {
        return writeInternal(b, ofs, len, handler, false);
    }

    @Override
    public boolean requiresFlush() {
        return writeBuffer.capacity() > writeBuffer.remaining();
    }

    @Override
    public boolean flush(AsyncHandler<Integer> handler) {
        boolean requiresFlush = writeBuffer.capacity() > writeBuffer.remaining();
        if(requiresFlush) {
            try {
                writeBuffer.flip();
                channel.write(writeBuffer, config.getWriteTimeoutMillis(), TimeUnit.MILLISECONDS, handler, flushCompletionHandler);
            }
            catch (Exception e) {
                AsyncRedisConnection.bootstrapExecutor.execute(new SendFailedTask(flushCompletionHandler, e, handler));
            }
        }

        return requiresFlush;
    }

    @Override
    public void close() throws IOException {
        if(channel != null) {
            channel.close();
            channel = null;
        }
        if(readBuffer != null) {
            readBuffer.clear();
        }
        if(writeBuffer != null) {
            writeBuffer.clear();
        }
        if(connectionStatus != ConnectionStatus.DOWN) {
            connectionStatus = ConnectionStatus.CLOSED;
        }
    }

}
