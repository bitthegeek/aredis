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

package org.aredis.test.io;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.aredis.cache.AsyncRedisConnection;
import org.aredis.cache.ConnectionType;
import org.aredis.cache.RedisClassDescriptorStorage;
import org.aredis.io.OptiObjectInputStream;
import org.aredis.io.OptiObjectOutputStream;
import org.aredis.io.ReusableByteArrayOutputStream;
import org.aredis.net.AsyncJavaSocketTransport;
import org.aredis.net.AsyncSocketTransportFactory;

public class TestOptiObjectStreams {

    public static int compareObjects(Comparable o1, Comparable o2) {
        int result = 0;
        if(o1 != o2) {
            if(o1 == null) {
                result = -1;
            }
            else if(o2 == null) {
                result = 1;
            }
            else {
                result = o1.compareTo(o2);
            }
        }

        return result;
    }

    public static boolean mapEquals(Map<String, ?> m1, Map<String, ?> m2) {
        boolean result = false;
        if(m1 != null) {
            if(m2 != null) {
                if(m1.size() == m2.size()) {
                    result = true;
                    for(String k1 : m1.keySet()) {
                        Object v1 = m1.get(k1);
                        if(m2.containsKey(k1)) {
                            Object v2 = m2.get(k1);
                            if(v1 != null) {
                                if(v1 instanceof Map && v2 instanceof Map) {
                                    if(!mapEquals((Map<String, Object>) v1, (Map<String, Object>) v2)) {
                                        result = false;
                                        break;
                                    }
                                }
                                else if(!v1.equals(v2)) {
                                    result = false;
                                    break;
                                }
                            }
                            else if(v2 != null) {
                                result = false;
                                break;
                            }
                        }
                    }
                }
            }
        }
        else if(m2 == null) {
            result = true;
        }

        return result;
    }

    private static Random r = new Random();

    public static class TestClass implements Serializable {
        private static final long serialVersionUID = -5896498492027243946L;

        public String s;
        public long l;
        public Date d = new Date();
        public byte b[];

        public TestClass() {
            int i = r.nextInt(8);
            if(i > 1) {
                s = String.valueOf(r.nextLong());
            }
            else if(i == 1) {
                s = "";
            }
        }

        @Override
        public boolean equals(Object o) {
            boolean result = false;
            if(o != null && o instanceof TestClass) {
                TestClass t = (TestClass) o;
                result = l == t.l && compareObjects(s, t.s) == 0 && compareObjects(d, t.d) == 0;
            }

            return result;
        }
    }

    public static class C1 implements Serializable {
        private Map<String, String> sMap;
        private int ia[];
        private TestClass t;

        @Override
        public boolean equals(Object o) {
            boolean result = false;
            if(o != null && o instanceof C1) {
                C1 c1 = (C1) o;
                if(t != null) {
                    if(t.equals(c1.t)) {
                        result = true;
                    }
                }
                else if(c1.t == null) {
                    result = true;
                }
                result = result && mapEquals(sMap, c1.sMap) && Arrays.equals(ia, c1.ia);
            }

            return result;
        }
    }

    private static class C2 implements Serializable {
        private String s;
        private List<TestClass> tcl;
        private Map<String, C1> c1s;

        @Override
        public boolean equals(Object o) {
            boolean result = false;
            if(o != null && o instanceof C1) {
                C2 c2 = (C2) o;
                if(compareObjects(s, c2.s) == 0 && mapEquals(c1s, c2.c1s)) {
                    if(tcl != null && c2.tcl != null) {
                        if(Arrays.equals(tcl.toArray(), c2.tcl.toArray())) {
                            result = true;
                        }
                    }
                    else if(tcl == null && c2.tcl == null) {
                        result = true;
                    }
                }
            }

            return result;
        }
    }

    public static void main(String [] args) throws Exception {
        String host = "localhost";
        int port = 6379;
        if(args.length > 0) {
            host = args[0];
        }
        if(args.length > 1) {
            port = Integer.parseInt(args[1]);
        }
        AsyncJavaSocketTransport socketTransport = new AsyncJavaSocketTransport(host, port, AsyncSocketTransportFactory.getDefault());
        AsyncRedisConnection aredis = new AsyncRedisConnection(socketTransport, 0, null, ConnectionType.STANDALONE);
        RedisClassDescriptorStorage ds = new RedisClassDescriptorStorage(aredis);
        ReusableByteArrayOutputStream bop = new ReusableByteArrayOutputStream();
        OptiObjectOutputStream oo = new OptiObjectOutputStream(bop, ds);
        TestClass tc = new TestClass();
        tc.s = "Hello";
        tc.l = 144;
        oo.writeObject(tc);
        ByteArrayInputStream bip = new ByteArrayInputStream(bop.getBuf());
        OptiObjectInputStream oip = new OptiObjectInputStream(bip, ds);
        TestClass tc1 = (TestClass) oip.readObject();
        System.out.println("Got Serialized class " + tc1);
        if(tc1 == tc) {
            System.out.println("Should not be equal");
        }
        if(!tc.s.equals(tc1.s)) {
            System.out.println("tc1.s " + tc1.s + " is not equal");
        }
        if(!tc.d.equals(tc1.d)) {
            System.out.println("tc1.d " + tc1.d + " is not equal");
        }
        if(tc.l != tc1.l) {
            System.out.println("tc1.l " + tc1.l + " is not equal");
        }
    }

}
