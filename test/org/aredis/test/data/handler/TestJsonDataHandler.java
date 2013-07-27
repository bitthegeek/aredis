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

import java.util.Arrays;
import java.util.List;

import org.aredis.cache.AsyncRedisConnection;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;

import com.fasterxml.jackson.core.type.TypeReference;

public class TestJsonDataHandler {

    private int i = 123;

    private String l[];

    public TestJsonDataHandler() {
        l = new String[30];
        for(int i = 0; i < l.length; i++) {
            l[i] = "l" + i;
        }
    }

    @Override
    public boolean equals(Object o) {
        boolean result = false;
        if(o instanceof TestJsonDataHandler) {
            TestJsonDataHandler t = (TestJsonDataHandler) o;
            result = i == t.i && Arrays.equals(l, t.l);
        }
        return result;
    }

    public int getI() {
        return i;
    }

    public void setI(int pi) {
        i = pi;
    }

    public String[] getL() {
        return l;
    }

    public void setL(String[] pl) {
        l = pl;
    }

    public static void main(String [] args) throws Exception {
        int i, len;
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        String host = "localhost";
        AsyncRedisConnection con = (AsyncRedisConnection) f.getClient(host);
        JsonHandler jh = new JsonHandler();
        jh.setCompressionThreshold(100);
        List<String> l = Arrays.asList("i0", "i1");
        con.submitCommand(jh, RedisCommand.SETEX, "hello", "300", l);
        String s = (String) con.submitCommand(RedisCommand.GET, "hello").get().getResult();
        System.out.println("Got json as: " + s);
        TypeReference<List<String>> tr = new TypeReference<List<String>>() {
        };
        List<String> tl = (List<String>) con.submitCommand(jh, tr, RedisCommand.GET, "hello").get().getResult();
        len = l.size();
        for(i = 0; i < len; i++) {
            if(!l.get(i).equals(tl.get(i))) {
                System.out.println("Got: " + tl.get(i) + " instead of " + l.get(i));
            }
        }
        TestJsonDataHandler t = new TestJsonDataHandler();
        con.submitCommand(jh, RedisCommand.SETEX, "hello", "300", t);
        s = (String) con.submitCommand(RedisCommand.GET, "hello").get().getResult();
        System.out.println("Got 2nd json as: " + s);
        TestJsonDataHandler t1 = (TestJsonDataHandler) con.submitCommand(jh, TestJsonDataHandler.class, RedisCommand.GET, "hello").get().getResult();
        if(!t.equals(t1)) {
            System.out.println("t1 = " + t1 + " != t");
        }
        con.shutdown(1);
    }
}
