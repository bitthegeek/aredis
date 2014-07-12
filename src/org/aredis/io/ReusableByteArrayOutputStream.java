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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * A GC friendly ByteArrayOutputStream which has a getInputStream method to read the
 * internal byte array without having to copy the contents.
 * @author Suresh
 *
 */
public class ReusableByteArrayOutputStream extends ByteArrayOutputStream {

    /**
     * Creates a ByteArrayOutputStream by calling the super class's constructor.
     */
    public ReusableByteArrayOutputStream() {
        super();
    }

    /**
     * Creates a ByteArrayOutputStream by calling the super class's constructor.
     * @param   size   the initial size.
     * @exception  IllegalArgumentException if size is negative.
     */
    public ReusableByteArrayOutputStream(int size) {
        super(size);
    }

    /**
     * Resets the stream and in addition limits the internal buffer's size to maxBufSize.
     * @param maxBufSize Max Buf Size after reset
     */
    public void reset(int maxBufSize) {
        super.reset();
        if(maxBufSize > 0) {
            if(maxBufSize < 32) {
                maxBufSize = 32;
            }
            if(buf != null && buf.length > maxBufSize) {
                buf = new byte[maxBufSize];
            }
        }
    }

    /**
     * Gets an Input Stream from which one can read the contents of the data written to the stream.
     * @return Input Stream for reading data
     */
    public ByteArrayInputStream getInputStream() {
        ByteArrayInputStream bip = new ByteArrayInputStream(buf, 0, count);
        return bip;
    }

    /**
     * Current count of the bytes in the buffer. The data starts from index 0.
     * @return Count of bytes written
     */
    public int getCount() {
        return count;
    }

    /**
     * Sets the count value. This is a low level functionality for manipulating the stream.
     * @param pcount
     */
    public void setCount(int pcount) {
        if(pcount < 0 || pcount > buf.length) {
            throw new IndexOutOfBoundsException(pcount + " is not in the range [0-" + buf.length + ']');
        }
        count = pcount;
    }

    /**
     * Gets the underlying buffer. This is again a low level functionality.
     * @return Internal buffer being used which might be replaced when capacity is expanded
     */
    public byte [] getBuf() {
        return buf;
    }

    /**
     * Sets a new buffer and resets the stream. This is also a low level functionality.
     * @param b New buffer to use
     */
    public void setBuf(byte [] b) {
        buf = b;
        reset();
    }
}
