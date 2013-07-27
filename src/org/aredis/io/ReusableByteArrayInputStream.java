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

/**
 * A ByteArrayInputStream which gives access to the current positions to provide more flexibility.
 * @author Suresh
 *
 */
public class ReusableByteArrayInputStream extends ByteArrayInputStream {


    /**
     * Creates a ByteArrayInputStream by calling the super class's constructor.
     * @param   buf   the input buffer.
     */
    public ReusableByteArrayInputStream(byte[] buf) {
        super(buf);
    }

    /**
     * Creates a ByteArrayInputStream by calling the super class's constructor.
     * @param   buf      the input buffer.
     * @param   offset   the offset in the buffer of the first byte to read.
     * @param   length   the maximum number of bytes to read from the buffer.
     */
    public ReusableByteArrayInputStream(byte[] buf, int offset, int length) {
        super(buf, offset, length);
    }

    /**
     * Sets the buffer to use.
     * @param   b      the input buffer.
     * @param   offset   the offset in the buffer of the first byte to read.
     * @param   length   the maximum number of bytes to read from the buffer.
     */
    public void setBuf(byte [] b, int offset, int length) {
        buf = b;
        pos = offset;
        count = Math.min(offset + length, buf.length);
        mark = offset;
    }

    /**
     * Gets the current position (offset) of the Stream.
     * @return Current possition
     */
    public int getPos() {
        return pos;
    }

    /**
     * Gets the current last position of the Stream till which there is data
     * @return The count which means there is data till count-1
     */
    public int getCount() {
        return count;
    }
}
