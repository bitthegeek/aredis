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

package org.aredis.io;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a class to reuse CompressibleByteArrayOutputStream to avoid creating a new one every time. This is not used
 * currently.
 * @author Suresh
 *
 */
public final class CompressibleByteArrayOutputStreamPool {

    public static final CompressibleByteArrayOutputStreamPool SINGLE_INSTANCE = new CompressibleByteArrayOutputStreamPool();

    private volatile int initBufSize;

    private volatile int compressionThreshold;

    private volatile int maxMemoryBytes;

    private AtomicInteger currentMemory;

    private Queue<CompressibleByteArrayOutputStream> pool;

    public CompressibleByteArrayOutputStreamPool() {
        initBufSize = 10240;
        compressionThreshold = CompressibleByteArrayOutputStream.DEFAULT_COMPRESSION_THRESHOLD;
        pool = new ConcurrentLinkedQueue<CompressibleByteArrayOutputStream>();
        currentMemory = new AtomicInteger();
        setMemoryLimitKb(5000);
    }

    public CompressibleByteArrayOutputStream borrowStream() {
        CompressibleByteArrayOutputStream stream = pool.poll();
        int threshold = compressionThreshold;
        if(stream == null) {
            int size = initBufSize;
            stream = new CompressibleByteArrayOutputStream(size, threshold);
        }
        else {
            stream.reset();
            stream.setCompressionEnabled(false);
            stream.setCompressionThreshold(threshold);
            currentMemory.addAndGet(-stream.getBuf().length);
        }

        return stream;
    }

    public boolean returnStream(CompressibleByteArrayOutputStream stream) {
        boolean success = false;
        int bufLen = stream.getBuf().length;
        if(stream != null && (currentMemory.get() + bufLen) <= maxMemoryBytes) {
            currentMemory.addAndGet(bufLen);
            success = pool.offer(stream);
        }

        return success;
    }

    public int getInitBufSize() {
        return initBufSize;
    }

    public void setInitBufSize(int pinitBufSize) {
        initBufSize = pinitBufSize;
    }

    public int getCompressionThreshold() {
        return compressionThreshold;
    }

    public void setCompressionThreshold(int pcompressionThreshold) {
        compressionThreshold = pcompressionThreshold;
    }

    public int getMaxMemoryLimitKb() {
        return maxMemoryBytes / 1000;
    }

    public void setMemoryLimitKb(int memoryLimit) {
        maxMemoryBytes = memoryLimit * 1000;
    }
    public Queue<CompressibleByteArrayOutputStream> getPool() {
        return pool;
    }

    public CompressibleByteArrayOutputStreamPool getInstance() {
        return SINGLE_INSTANCE;
    }

}
