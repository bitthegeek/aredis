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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.BinaryHandler;
import org.aredis.cache.JavaHandler;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;
import org.aredis.cache.StringHandler;
import org.aredis.test.io.TestOptiObjectStreams;
import org.aredis.test.io.TestOptiObjectStreams.TestClass;

public class TestDataHandlers {

    private static String prefixChars(String s, int ch1, int ch2) throws UnsupportedEncodingException {
        byte b[] = s.getBytes(Charset.defaultCharset());
        int i = b.length + 2;
        if(ch2 != -1) {
            i += 2;
        }
        byte b1[] = new byte[i];
        b1[0] = (byte) (ch1 >> 8);
        b1[1] = (byte) ch1;
        i = 2;
        if(ch2 != -1) {
            b1[2] = (byte) (ch2 >> 8);
            b1[3] = (byte) ch2;
            i = 4;
        }
        System.arraycopy(b, 0, b1, i, b.length);
        return new String(b1, Charset.defaultCharset());
    }

    private static void checkString(String s, int i) {
        byte b[] = s.getBytes(Charset.defaultCharset());
        String s1 = new String(b, Charset.defaultCharset());
        if(!s.equals(s1)) {
            System.out.println("CHECK MISMATCH for index " + i);
        }
    }

    public static void main(String [] args) throws Exception {
        int i;
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        String host = "localhost";
        TestClass tc = new TestOptiObjectStreams.TestClass();
        AsyncRedisClient con = f.getClient(host);
        String s, s1;
        String shortString = "1234567890";
        String longString = "000000000000000111111111111111222222222222222333333333333333444444444444444555555555555555666666666666666777777777777777888888888888888999999999999999";
        String testStrings[] = new String[] {
            shortString,
            longString,
            prefixChars(shortString, JavaHandler.ESCAPE_MARKER, 0),
            prefixChars(longString, JavaHandler.ESCAPE_MARKER, 0),
            prefixChars(shortString, JavaHandler.GZIP_HEADER, -1),
            prefixChars(longString, JavaHandler.GZIP_HEADER, -1)
        };
        JavaHandler jh = new JavaHandler();
        jh.setCharEncoding(Charset.defaultCharset().name());
        jh.setStringCompressionThreshold(100);
        StringHandler sh = new StringHandler();
        StringHandler sh1 = new StringHandler(true);
        for(i = 0; i < testStrings.length; i++) {
            s = testStrings[i];
            checkString(s, i);
            con.submitCommand(new RedisCommandInfo(jh, RedisCommand.SETEX, "hello", "300", s)).get();
            s1 = (String) con.submitCommand(new RedisCommandInfo(jh, RedisCommand.GET, "hello")).get().getResult();
            if(!s.equals(s1)) {
                System.out.println("String mismatch for Java Handler for index: " + i);
            }
            con.submitCommand(new RedisCommandInfo(sh, RedisCommand.SETEX, "hello", "300", s)).get();
            s1 = (String) con.submitCommand(new RedisCommandInfo(sh, RedisCommand.GET, "hello")).get().getResult();
            if(!s.equals(s1)) {
                System.out.println("String mismatch for String Handler for index: " + i);
            }
            con.submitCommand(new RedisCommandInfo(sh1, RedisCommand.SETEX, "hello", "300", s)).get();
            s1 = (String) con.submitCommand(new RedisCommandInfo(sh1, RedisCommand.GET, "hello")).get().getResult();
            if(!s.equals(s1)) {
                System.out.println("String mismatch for Compressed String Handler for index: " + i);
            }
        }
        FileInputStream fip = new FileInputStream("G:/pictures/out.jpg");
        byte buf[] = new byte[4096];
        ByteArrayOutputStream bop = new ByteArrayOutputStream();
        while((i = fip.read(buf)) > 0) {
            bop.write(buf, 0, i);
        }
        fip.close();
        tc.b = bop.toByteArray();
        BinaryHandler bh = new BinaryHandler(true);
        con.submitCommand(new RedisCommandInfo(bh, RedisCommand.SETEX, "hello", "300", tc.b));
        byte b[] = (byte []) con.submitCommand(new RedisCommandInfo(bh, RedisCommand.GET, "hello")).get().getResult();
        if(!Arrays.equals(tc.b, b)) {
            System.out.println("Binary Data handler result does not match");
        }
        con.submitCommand(jh, RedisCommand.SETEX, "hello", "300", 123);
        i = con.submitCommand(RedisCommand.INCR, "hello").get().getIntResult(0);
        i = con.submitCommand(jh, RedisCommand.GET, "hello").get().getIntResult(0);
        if(i != 124) {
            System.out.println("Got result of incr: " + i + " instead of 124");
        }
        /*
        tc1 = (TestClass) con.submitCommand(new RedisCommandInfo(jh, RedisCommand.GET, "hello")).get().getResult();
        System.out.println("Got tc1 = " + tc1);
        */
    }
}
