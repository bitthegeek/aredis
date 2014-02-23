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

package org.aredis.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Generic util class containing static methods.
 *
 * @author Suresh
 *
 */
public class GenUtil {

    /**
     * Milliseconds in a day.
     */
    public static final long MILLIS_IN_A_DAY = 24 * 3600 * 1000;

    /**
     * Add a debug message to a StringBuffer with a new line.
     * @param debugBuf Debug Buffer which can be null
     * @param message Message to write
     */
    public static void addDebug(StringBuffer debugBuf, String message) {
        if(debugBuf != null) {
            debugBuf.append(message).append('\n');
        }
    }

    /**
     * Computes digest of given input String.
     * @param s Input String
     * @param algorithm Algorithm to use. Examples: SHA-1, SHA-256, MD5.
     * @return Hex encoded digest
     * @throws NoSuchAlgorithmException if algorithm is invalid
     */
    public static String digest(String s, String algorithm) throws NoSuchAlgorithmException {
        int i;
        MessageDigest md = MessageDigest.getInstance(algorithm);

        byte[] hash = md.digest(s.getBytes());

        StringBuilder sb = new StringBuilder(2 * hash.length);
        for(i = 0; i < hash.length; i++) {
            int b = hash[i] & 0xff;
            if(b < 16) {
                sb.append('0');
            }
            sb.append(Integer.toHexString(b));
        }

        return sb.toString();
    }

}
