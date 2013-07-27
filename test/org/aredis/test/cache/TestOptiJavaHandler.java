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

package org.aredis.test.cache;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;
import org.aredis.io.ReusableByteArrayOutputStream;
import org.aredis.test.io.TestOptiObjectStreams;
import org.aredis.test.io.TestOptiObjectStreams.TestClass;

public class TestOptiJavaHandler extends Thread {

    private AsyncRedisClient con;

    private Object objs[];

    private String keys[];

    private int count;

    private byte ser[][];

    private static class Watcher extends Thread {
        Thread watchedThreads[];

        public Watcher(Thread pwatchedThreads[]) {
            watchedThreads = pwatchedThreads;
        }

        public void run() {
            try {
                Thread.sleep(100000);
                int i;
                for(i = 0; i < watchedThreads.length; i++) {
                    if(watchedThreads[i].isAlive()) {
                        watchedThreads[i].dumpStack();
                    }
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public TestOptiJavaHandler(AsyncRedisClient pcon, String pkeys[], Object pobjs[], int pcount) throws IOException, ClassNotFoundException {
        con = pcon;
        objs = pobjs;
        keys = pkeys;
        count = pcount;
        ser = new byte[objs.length][];
        int i;
        ReusableByteArrayOutputStream bop = new ReusableByteArrayOutputStream();
        for(i = 0; i < objs.length; i++) {
            bop.reset();
            ObjectOutputStream oop = new ObjectOutputStream(bop);
            oop.writeObject(objs[i]);
            oop.close();
            ObjectInputStream oip = new ObjectInputStream(bop.getInputStream());
            Object o1 = oip.readObject();
            bop.reset();
            oop = new ObjectOutputStream(bop);
            oop.writeObject(o1);
            oop.close();
            ser[i] = bop.toByteArray();
        }
    }

    public void run() {
        int i, iteration;
        try {
            ReusableByteArrayOutputStream bop = new ReusableByteArrayOutputStream();
            for(iteration = 0; iteration < count; iteration++) {
                for(i = 0; i < objs.length; i++) {
                    if(keys[i].startsWith("hello")) {
                        i = i + 3 - 3;
                    }
                    con.submitCommand(new RedisCommandInfo(RedisCommand.SETEX, keys[i], 300, objs[i]));
                    Object obj = con.submitCommand(new RedisCommandInfo(RedisCommand.GET, keys[i])).get(300, TimeUnit.SECONDS).getResult();
                    bop.reset();
                    ObjectOutputStream oop = new ObjectOutputStream(bop);
                    oop.writeObject(obj);
                    oop.close();
                    byte[] b = bop.toByteArray();
                    if(!Arrays.equals(b, ser[i])) {
                        System.out.println("Comparison failed for key = " + keys[i] + " obj = " + obj + " o = " + objs);
                    }
                }
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void main1(String [] args) throws Exception {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        String host = "localhost";
        AsyncRedisClient con = f.getClient(host);
        AsyncRedisClient con1 = f.getClient(host + ":6379");
        // ((AsyncRedisConnection) con).setDataHandler(AsyncRedisConnection.JAVA_HANDLER);
        if(con != con1) {
            System.out.println("ERROR: Connection should be same with or without default port");
        }
        TestClass tc = new TestOptiObjectStreams.TestClass();
        con.submitCommand(new RedisCommandInfo(RedisCommand.SETEX, "hello", 30, tc));
        TestClass tc1 = (TestClass) con.submitCommand(new RedisCommandInfo(RedisCommand.GET, "hello")).get(60, TimeUnit.SECONDS).getResult();
        if(tc1 == tc) {
            System.out.println("Should not be equal");
        }
        if(tc.s != tc1.s && !tc.s.equals(tc1.s)) {
            System.out.println("tc1.s " + tc1.s + " is not equal");
        }
        if(!tc.d.equals(tc1.d)) {
            System.out.println("tc1.d " + tc1.d + " is not equal");
        }
        if(tc.l != tc1.l) {
            System.out.println("tc1.l " + tc1.l + " is not equal");
        }
        System.out.println("Test Completed");
    }

    private static final String SER_GZ_SUFFIX = ".ser.gz";

    private static void loadFromFiles(String dir, List<String> keys, List<Object> objs) throws IOException, ClassNotFoundException {
        File folder = new File(dir);
        File[] listOfFiles = folder.listFiles();
        int i, suffixLen = SER_GZ_SUFFIX.length();

        for (i = 0; i < listOfFiles.length; i++) {
            File f = listOfFiles[i];
            if (f.isFile()) {
                String fname = f.getName();
                if(fname.endsWith(SER_GZ_SUFFIX)) {
                    String key = fname.substring(0, fname.length() - suffixLen);
                    InputStream fip = new FileInputStream(f);
                    try {
                        ObjectInputStream oip = new ObjectInputStream(new GZIPInputStream(new BufferedInputStream(fip)));
                        Object obj = oip.readObject();
                        keys.add(key);
                        objs.add(obj);
                    }
                    finally {
                        fip.close();
                    }
                }
           }
        }
    }

    public static void main2(String [] args) throws Exception {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        String host = "localhost";
        AsyncRedisClient con = f.getClient(host);
        Object keys[] = (Object []) con.submitCommand(new RedisCommandInfo(RedisCommand.KEYS, "*")).get().getResult();
        for(Object keyObj : keys) {
            String key = (String) keyObj;
            if(key.indexOf("_I") > 0) {
                Object result = con.submitCommand(new RedisCommandInfo(RedisCommand.GET, keyObj)).get().getResult();
                System.out.println("Key: " + keyObj + " val: " + result);
            }
        }
    }

    public static void main(String [] args) throws Exception {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        String host = "localhost";
        AsyncRedisClient con = f.getClient(host);
        int i, j, threadCount = 10;
        TestOptiJavaHandler tests[] = new TestOptiJavaHandler[threadCount];
        TestClass tc = new TestOptiObjectStreams.TestClass();
        List<String> keyList = new ArrayList<String>();
        List<Object> objList = new ArrayList<Object>();
        loadFromFiles("D:/backup/cache_data", keyList, objList);
        keyList.add("hello");
        objList.add(tc);
        Object objs[] = objList.toArray();
        String instanceSuffix = "i0";
        if(args.length > 0) {
            instanceSuffix = args[0];
        }
        for(i = 0; i < threadCount; i++) {
            String keys[] = keyList.toArray(new String[keyList.size()]);
            for(j = 0; j < keys.length; j++) {
                keys[j] = keys[j] + '_' + instanceSuffix + '_' + i;
            }
            tests[i] = new TestOptiJavaHandler(con, keys, objs, 1);
        }
        // Watcher watcher = new Watcher(tests);
        // watcher.setDaemon(true);
        // watcher.start();
        for(i = 0; i < threadCount; i++) {
            tests[i].start();
        }
        for(i = 0; i < threadCount; i++) {
            tests[i].join();
        }
        System.out.println("Test Completed");
    }

}
