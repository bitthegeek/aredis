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

/**
 * Specifies the Configuration of AsyncSocketTransport. The config is used typically when the connection is created
 * and the timeouts for each read and write.
 * @author suresh
 *
 */
public class AsyncSocketTransportConfig implements Cloneable {

    /**
     * Default Read/Write Buf Size.
     */
    public static final int DEFAULT_BUF_SIZE = 8192;

    /**
     * Default read/write TIMEOUT which is set to 1 min.
     */
    public static final long DEFAULT_TIMEOUT_MILLIS = 60000; // 1 min

    /**
     * Default idle time for marking the connection stale which is set to 15 min.
     */
    public static final long DEFAULT_MAX_IDLE_TIME = 900000; // 15 min

    /**
     * Read Buffer Size.
     */
    protected int readBufSize;

    /**
     * Write Buffer Size.
     */
    protected int writeBufSize;

    /**
     * Read Timeout in milliseconds.
     */
    protected long readTimeoutMillis;

    /**
     * Write Timeout in milliseconds.
     */
    protected long writeTimeoutMillis;

    /**
     * Max idle Timeout in milliseconds.
     */
    protected volatile long maxIdleTimeMillis;

    /**
     * Creates a Config with custom parameters.
     * @param preadBufSize Read Buffer Size
     * @param pwriteBufSize Write Buffer Size
     * @param preadTimeoutMillis Read Timeout in milliseconds
     * @param pwriteTimeoutMillis Write Timeout in milliseconds
     * @param pmaxIdleTimeMillis Max idle Timeout in milliseconds
     */
    public AsyncSocketTransportConfig(int preadBufSize, int pwriteBufSize,
            long preadTimeoutMillis, long pwriteTimeoutMillis, long pmaxIdleTimeMillis) {
        readBufSize = preadBufSize;
        writeBufSize = pwriteBufSize;
        readTimeoutMillis = preadTimeoutMillis;
        writeTimeoutMillis = pwriteTimeoutMillis;
        maxIdleTimeMillis = pmaxIdleTimeMillis;
    }

    /**
     * Creates a config with default parameters.
     */
    public AsyncSocketTransportConfig() {
        this(DEFAULT_BUF_SIZE, DEFAULT_BUF_SIZE, DEFAULT_TIMEOUT_MILLIS, DEFAULT_TIMEOUT_MILLIS, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates a copy of this config in case a local change is to be done without affecting other connections.
     */
    public AsyncSocketTransportConfig clone() {
        AsyncSocketTransportConfig copy = null;
        try {
            copy = (AsyncSocketTransportConfig) super.clone();
        } catch (CloneNotSupportedException e) {
        }
        return copy;
    }

    public int getReadBufSize() {
        return readBufSize;
    }

    public void setReadBufSize(int preadBufSize) {
        readBufSize = preadBufSize;
    }

    public int getWriteBufSize() {
        return writeBufSize;
    }

    public void setWriteBufSize(int pwriteBufSize) {
        writeBufSize = pwriteBufSize;
    }

    public long getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    public void setReadTimeoutMillis(long preadTimeoutMillis) {
        readTimeoutMillis = preadTimeoutMillis;
    }

    public long getWriteTimeoutMillis() {
        return writeTimeoutMillis;
    }

    public void setWriteTimeoutMillis(long pwriteTimeoutMillis) {
        writeTimeoutMillis = pwriteTimeoutMillis;
    }

    public long getMaxIdleTimeMillis() {
        return maxIdleTimeMillis;
    }

    public void setMaxIdleTimeMillis(long pmaxIdleTimeMillis) {
        maxIdleTimeMillis = pmaxIdleTimeMillis;
    }

}
