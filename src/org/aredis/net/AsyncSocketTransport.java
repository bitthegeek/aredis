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

import java.io.IOException;
import java.io.OutputStream;
import org.aredis.cache.AsyncHandler;
import org.aredis.cache.AsyncRedisConnection;

/**
 * Specifies an Async Socket Transport interface that can be used by {@link AsyncRedisConnection} to connect to a Redis Server.
 * @author suresh
 *
 */
public interface AsyncSocketTransport extends ServerInfo {

    /**
     * Connects to Socket and calls handler with true on success and false with
     * Exception on failure.
     * @param handler Handler to call
     */
    void connect(AsyncHandler<Boolean> handler);

    /**
     * Reads atleast 1 byte and a max of upto maxBytes writing bytes read to specified
     * Stream.
     * @param dest OutputStream to write bytes Read
     * @param maxBytes max Bytes to read
     * @param handler handler to call in case of Async operation
     * @return 0 if Operation will be completed by callback to handler with bytes read
     * or positive value indicating actual bytes read immediately in which case handler
     * will not be called.
     */
    int read(OutputStream dest, int maxBytes, AsyncHandler<Integer> handler);

    /**
     * Writes specified bytes to Output Stream.
     * @param b Byte Array containing bytes to write
     * @param ofs offset to write from
     * @param len number of bytes to write
     * @param handler handler to call in case of Async operation. The handler is
     * called after all bytes have been written with the number of bytes actaully
     * transferred to the Socket Output.
     * @return len if all bytes can be transferred immediately to local buffer in
     * in which case handler will not be called. 0 if it requires an Async operation
     * in which case handler is called on completion with bytes actually transferred
     * to socket Output (The remaining bytes will be in buffer).
     */
    int write(byte [] b, int ofs, int len, AsyncHandler<Integer> handler);

    /**
     * Whether the Socket Output requires a flush to transfer bytes in buffer.
     * @return true if flush is required
     */
    boolean requiresFlush();

    /**
     * Flushes bytes in buffer to Socket Output. May require repeated calls.
     * @param handler Callback handler in case flush is required. The handler is
     * called with bytes transferred if further flush call is required or a
     * negative value of -bytes transferred if further flush call is not required.
     * @return true if flush is required, false if not required in which case handler
     * is not called.
     */
    boolean flush(AsyncHandler<Integer> handler);

    /**
     * Closes the Socket connection. Can be opened again by calling connect.
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * Returns the Connection Status
     * @return Connection Status
     */
    ConnectionStatus getStatus();

    /**
     * Checks if the connection is stale as per configured idle timeout. This method differs from getStatus in that it checks only for the
     * last use time and not the current connection state if it is OK, DOWN or CLOSED.
     * @return True if the connection is Stale
     */
    boolean isStale();

    /**
     * Gets the AsyncSocketTransport config in use.
     * @return config in use
     */
    AsyncSocketTransportConfig getConfig();

    /**
     * Sets the AsyncSocketTransport config to use.
     * @param pconfig to use
     */
    void setConfig(AsyncSocketTransportConfig pconfig);

    /**
     * Returns the millisecond time since this connection was down. This could be common to all connections to the same server.
     * @return millisecond time since this connection was down, 0 if the connection is not down
     */
    long getDownSince();

    /**
     * The current retry interval as per the exponential fall back calculation. This could be common to all connections to the same server.
     * @return Current retry interval
     */
    long getRetryInterval();
}
