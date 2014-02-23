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

package org.aredis.cache;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.aredis.io.CompressibleByteArrayOutputStream;
import org.aredis.net.ServerInfo;

/**
 * A Data Handler which stores a Byte Array of data optionally compressing it. The compression is either on or off and
 * not based on a threshold.
 * @author Suresh
 *
 */
public class BinaryHandler implements DataHandler {

    private boolean isCompressed;

    /**
     * Creates a Binary Handler.
     * @param pisCompressed If the binary data should be compressed
     */
    public BinaryHandler(boolean pisCompressed) {
        isCompressed = pisCompressed;
    }

    public BinaryHandler() {
        this(false);
    }

    /**
     * The data to serialize must be a byte array.
     */
    @Override
    public void serialize(Object data, Object metaData, CompressibleByteArrayOutputStream op, ServerInfo serverInfo) throws IOException {
        op.setCompressionEnabled(isCompressed);
        op.setAbandonCompressionEnabled(false);
        int savedThreshold = op.getCompressionThreshold();
        op.setCompressionThreshold(0);
        try {
            op.write((byte []) data);
        }
        finally {
            op.setCompressionThreshold(savedThreshold);
        }
    }

    @Override
    public Object deserialize(Object metaData, byte [] b, int offset, int len, ServerInfo serverInfo) throws IOException {
        if(offset < 0 || len <= 0 || offset + len > b.length) {
            throw new IndexOutOfBoundsException("Invalid values, ofs: " + offset + ", len: " + len + ", b.length: " + b.length);
        }

        Object o = null;

        if(!isCompressed) {
            o = new byte[len];
            System.arraycopy(b, offset, o, 0, len);
        }
        else {
            InputStream ip = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(b, offset, len)), 2048);
            int bytesRead;
            byte [] buf = new byte[128];
            ByteArrayOutputStream bop = new ByteArrayOutputStream();
            while( (bytesRead = ip.read(buf)) > 0) {
                bop.write(buf, 0, bytesRead);
            }
            o = bop.toByteArray();
        }

        return o;
    }

}
