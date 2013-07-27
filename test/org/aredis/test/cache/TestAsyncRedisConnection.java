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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.aredis.cache.AccessUtil;
import org.aredis.cache.AsyncHandler;
import org.aredis.cache.AsyncRedisConnection;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;
import org.aredis.cache.RedisCommandInfo.CommandStatus;
import org.aredis.net.AsyncJavaSocketTransport;
import org.aredis.net.AsyncSocketTransportFactory;

public class TestAsyncRedisConnection extends Thread {

    private static final int KEYS_PER_THREAD = 100;

    private volatile static boolean stop;

    private String prefix;

    private AsyncRedisConnection aredis;

    private int count;

    private StringBuffer sb;

    public static class TestObj implements Serializable {
        public int n;
        String s = "Hello World";
    }

    public void run() {
        Random r = new Random();
        int counter = 0;
        int keysCounter = 0;
        try {
        do {
            sb = null;
            String key = prefix + counter;
            counter = (counter + 1) % KEYS_PER_THREAD;
            String val = String.valueOf(r.nextInt());
            RedisCommandInfo commandInfos[] = {new RedisCommandInfo(RedisCommand.SETEX, key, "30", val)};
            // commandInfo.setDebugBuf(new StringBuffer());
            aredis.submitCommands(commandInfos, null);
            boolean testKeys = false;
            if(counter == 0) {
                keysCounter = (keysCounter + 1) % 10;
                if(keysCounter == 0) {
                    testKeys = true;
                }
            }
            // testKeys = false;
            Future<RedisCommandInfo[]> keysResult = null;
            if(testKeys) {
                commandInfos = new RedisCommandInfo[] {new RedisCommandInfo(RedisCommand.KEYS, prefix + "*")};
                // commandInfo.setDebugBuf(new StringBuffer());
                keysResult = aredis.submitCommands(commandInfos);
            }
            commandInfos = new RedisCommandInfo[] {new RedisCommandInfo(RedisCommand.GET, key)};
            // commandInfo.setDebugBuf((sb = new StringBuffer()));
            Future<RedisCommandInfo[]> result = aredis.submitCommands(commandInfos);
            if(testKeys) {
                commandInfos = keysResult.get();
                assert ((List<String>) commandInfos[0].getResult()).size() == KEYS_PER_THREAD;
            }
            commandInfos = result.get();
            try {
                if(!val.equals(commandInfos[0].getResult())) {
                    throw new RuntimeException("Assertion Failed");
                }
            }
            catch(Throwable e) {
                System.out.println("Got " + commandInfos[0].getResult() + " instead of " + val);
                System.out.println("DEBUG:\n" + commandInfos[0].getDebugBuf());
                System.exit(1);
            }
            count++;
        } while(!stop);
    }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static class CompletionHandler implements AsyncHandler<RedisCommandInfo[]> {

        private String expectedResult;

        private int resultIndex;

        private int commandCount;

        private long startTime;

        AsyncRedisConnection aredis;

        private static AtomicInteger index = new AtomicInteger();

        @Override
        public void completed(RedisCommandInfo[] results, Throwable e) {
            if(index.incrementAndGet() < 20) {
                System.out.println("Here");
            }
            RedisCommandInfo result = results[resultIndex];
            if(expectedResult != null && result.getRunStatus() == CommandStatus.SUCCESS && !expectedResult.equals(result.getResult())) {
                System.out.println("Error: expecetd: " + expectedResult + " got: " + result.getResult());
            }
            if(commandCount > 0 && startTime > 0) {
                long diff = System.currentTimeMillis() - startTime;
                long rps = commandCount * 1000 / diff;
                System.out.println("Time = " + diff + ", total count = " + commandCount + " rps = " + rps + " maxPending = " + AccessUtil.getMaxPending(aredis) + " flushCount = " + AccessUtil.getFlushCount(aredis));
            }
        }

    }

    public static void main1(String [] args) throws InterruptedException, ExecutionException, IOException {
        long start = System.currentTimeMillis();
        final int dbIndex = 1;
        String host = "localhost";
        int port = 6379;
        if(args.length > 0) {
            host = args[0];
        }
        if(args.length > 1) {
            port = Integer.parseInt(args[1]);
        }
        AsyncSocketTransportFactory transportFactory = AsyncSocketTransportFactory.getDefault();
        AsyncJavaSocketTransport socketTransport = new AsyncJavaSocketTransport(host, port, transportFactory);
        AsyncRedisConnection aredis = new AsyncRedisConnection(socketTransport, dbIndex, null);
        int i, commandCount = 100000;
        i = 0;
        Random r = new Random(10);
        StringBuilder sb = new StringBuilder();
        sb.append("key_");
        int prefixLen = sb.length();
        List<RedisCommandInfo> cl = new ArrayList<RedisCommandInfo>(6);
        short dbIndexLookup [] = new short[] {dbIndex, 5, 7, 17};
        do {
            cl.clear();
            sb.setLength(prefixLen);
            sb.append(r.nextInt(50));
            String key = sb.toString();
            String val = String.valueOf(r.nextInt());
            int resultIndex = 1;
            int keyDbIndex = dbIndex;
            // Switch the below 2 lines to turn on/off SELECT DB testing
            // int flags = 0;
            int flags = r.nextInt(8);
            if((flags & 1) != 0) {
                resultIndex++;
                keyDbIndex = dbIndexLookup[r.nextInt(dbIndexLookup.length)];
                cl.add(new RedisCommandInfo(RedisCommand.SELECT, String.valueOf(keyDbIndex)));
            }
            cl.add(new RedisCommandInfo(RedisCommand.SETEX, key, "120", val));
            if((flags & 2) != 0) {
                resultIndex++;
                cl.add(new RedisCommandInfo(RedisCommand.SELECT, String.valueOf(keyDbIndex)));
            }
            cl.add(new RedisCommandInfo(RedisCommand.GET, key));
            if((flags & 4) != 0) {
                keyDbIndex = dbIndexLookup[r.nextInt(dbIndexLookup.length)];
                cl.add(new RedisCommandInfo(RedisCommand.SELECT, String.valueOf(keyDbIndex)));
            }
            RedisCommandInfo commandInfos[] = cl.toArray(new RedisCommandInfo[cl.size()]);
            CompletionHandler completionHandler = new CompletionHandler();
            completionHandler.aredis = aredis;
            completionHandler.expectedResult = val;
            completionHandler.resultIndex = resultIndex;
            i += commandInfos.length;
            if(i >= commandCount) {
                completionHandler.startTime = start;
                completionHandler.commandCount = i;
            }
            aredis.submitCommands(commandInfos, completionHandler);
        } while(i < commandCount);
        System.out.println("Time to send: " + (System.currentTimeMillis() - start) + " last receipt " + CompletionHandler.index);
        // aredis.shutdown(1);
        Thread.sleep(60000);
        System.out.println("Final receipt " + CompletionHandler.index.get() + " req " + AccessUtil.getRequestQueue(aredis).size() + " res " + AccessUtil.getResponseQueue(aredis).size());
    }

    public static void main(String [] args) throws InterruptedException, ExecutionException, IOException {
        long start = System.currentTimeMillis();
        String host = "localhost";
        int port = 6379;
        if(args.length > 0) {
            host = args[0];
        }
        if(args.length > 1) {
            port = Integer.parseInt(args[1]);
        }
        AsyncSocketTransportFactory transportFactory = AsyncSocketTransportFactory.getDefault();
        AsyncJavaSocketTransport socketTransport = new AsyncJavaSocketTransport(host, port, transportFactory);
        AsyncRedisConnection aredis = new AsyncRedisConnection(socketTransport, 0, null);
        int i, threadCount = 200;
        TestAsyncRedisConnection [] testThreads = new TestAsyncRedisConnection[threadCount];
        for(i = 0; i < threadCount; i++) {
            TestAsyncRedisConnection tr = new TestAsyncRedisConnection();
            tr.aredis = aredis;
            tr.prefix = "k" + i + '_';
            testThreads[i] = tr;
            testThreads[i].start();
        }
        Thread.sleep(20000);
        stop = true;
        // Thread.sleep(10000);
        // aredis.printDebug();
        // System.out.println(testThreads[0].sb);
        int total = 0;
        for(i = 0; i < threadCount; i++) {
            testThreads[i].join();
            System.out.println("Count " + i + ": " + testThreads[i].count);
            total += testThreads[i].count;
        }
        aredis.shutdown(1);
        long diff = System.currentTimeMillis() - start;
        System.out.println("Time = " + diff + ", total count = " + total + " maxPending = " + AccessUtil.getMaxPending(aredis) + " flushCount = " + AccessUtil.getFlushCount(aredis));
    }
}
