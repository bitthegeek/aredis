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

package org.aredis.test.data.handler;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

import org.aredis.cache.DataHandler;
import org.aredis.io.CompressibleByteArrayOutputStream;
import org.aredis.io.RedisConstants;
import org.aredis.io.ReusableByteArrayInputStream;
import org.aredis.net.ServerInfo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonHandler implements DataHandler {

    private static enum StreamType {STRING, GZIP};

    public static final int GZIP_HEADER = 0x1f8b;

    private int compressionThreshold;

    private ObjectMapper objectMapper;

    private Charset charEncoding;

    public JsonHandler() {
        charEncoding = RedisConstants.UTF_8_CHARSET;
        compressionThreshold = -2;
        objectMapper = new ObjectMapper();
    }

    protected void writeObject(CompressibleByteArrayOutputStream op, Object o) throws IOException {
        objectMapper.writeValue(op, o);
    }

    protected Object readObject(InputStream ip, Object metaData) throws IOException {
        Object o = null;
        if(metaData instanceof Class) {
            o = objectMapper.readValue(ip, (Class) metaData);
        }
        else if(metaData instanceof TypeReference) {
            o = objectMapper.readValue(ip, (TypeReference) metaData);
        }
        else if(metaData instanceof JavaType) {
            o = objectMapper.readValue(ip, (JavaType) metaData);
        }
        return o;
    }

    @Override
    public void serialize(Object data, Object metaData, CompressibleByteArrayOutputStream op, ServerInfo serverInfo) throws IOException {
        op.setCompressionEnabled(true);
        if(compressionThreshold >= -1) {
            op.setCompressionThreshold(compressionThreshold);
        }
        writeObject(op, data);
    }

    // Positions stream at a point ready to read Object or String after discarding ESCAPE_MARKER/Optimize Marker
    private StreamType checkObjectStream(InputStream ip) throws IOException {
        ip.mark(2);
        int firstTwoBytes = ip.read();
        firstTwoBytes = (firstTwoBytes << 8) | ip.read();
        StreamType status = StreamType.STRING;
        switch(firstTwoBytes) {
          case GZIP_HEADER:
            status = StreamType.GZIP;
            ip.reset();
            break;
          default:
            ip.reset();
            break;
        }

        return status;
    }

    @Override
    public Object deserialize(Object metaData, byte [] b, int offset, int len, ServerInfo serverInfo) throws IOException {
        if(offset < 0 || len <= 0 || offset + len > b.length) {
            throw new IndexOutOfBoundsException("Invalid values, ofs: " + offset + ", len: " + len + ", b.length: " + b.length);
        }
        ReusableByteArrayInputStream rip = new ReusableByteArrayInputStream(b, offset, len);
        InputStream ip = rip;
        Object o = null;
        StreamType streamType = checkObjectStream(ip);
        if(streamType == StreamType.GZIP) {
            ip = new BufferedInputStream(new GZIPInputStream(ip), 8);
        }
        o = readObject(ip, metaData);

        return o;
    }

    public String getCharEncoding() {
        return charEncoding.name();
    }

    public void setCharEncoding(String pcharEncoding) {
        charEncoding = Charset.forName(pcharEncoding);
    }

    public int getCompressionThreshold() {
        return compressionThreshold;
    }

    public void setCompressionThreshold(int pcompressionThreshold) {
        compressionThreshold = pcompressionThreshold;
    }
}
