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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

import org.aredis.io.CompressibleByteArrayOutputStream;
import org.aredis.net.ServerInfo;

/**
 * A Data Handler which stores a String optionally compressing it. The compression is either on or off and
 * not based on a threshold.
 * @author Suresh
 *
 */
public class StringHandler implements DataHandler {

    private String charEncoding;

    private Charset charset;

    private boolean isCompressed;

    public StringHandler(boolean pisCompressed) {
        charEncoding = "UTF-8";
        charset = Charset.forName(charEncoding);
        isCompressed = pisCompressed;
    }

    public StringHandler() {
        this(false);
    }

    /**
     * The data to serialize must be a String.
     */
    @Override
    public void serialize(Object data, Object metaData, CompressibleByteArrayOutputStream op, ServerInfo serverInfo) throws IOException {
        op.setCompressionEnabled(isCompressed);
        op.setAbandonCompressionEnabled(false);
        int savedThreshold = op.getCompressionThreshold();
        op.setCompressionThreshold(0);
        try {
            op.write(((String) data).getBytes(charset));
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
            o = new String(b, offset, len, charset);
        }
        else {
            InputStream ip = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(b, offset, len)), 2048);
            Reader r = new InputStreamReader(ip, charset);
            int charsRead;
            char [] cbuf = new char[128];
            StringBuilder sb = new StringBuilder(len*5); // Estimating 5x compression
            while( (charsRead = r.read(cbuf)) > 0) {
                sb.append(cbuf, 0, charsRead);
            }
            o = sb.toString();
        }

        return o;
    }

    public String getCharEncoding() {
        return charEncoding;
    }

    public void setCharEncoding(String pcharEncoding) {
        charEncoding = pcharEncoding;
        charset = Charset.forName(charEncoding);
    }

}
