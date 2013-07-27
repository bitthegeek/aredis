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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;
import org.aredis.util.concurrent.SingleConsumerQueue;

public class TestQueue extends Thread {

    private static final int KEYS_PER_THREAD = 100;

    private volatile static boolean stop;

    private String prefix;

    private int count;

    private SingleConsumerQueue<RedisCommandInfo> q;

    public void run() {
        Random r = new Random();
        int counter = 0;
        try {
            do {
                String key = prefix + counter;
                counter = (counter + 1) % KEYS_PER_THREAD;
                String val = String.valueOf(r.nextInt());
                RedisCommandInfo commandInfo = new RedisCommandInfo(RedisCommand.SETEX, key, "30", val);
                q.add(commandInfo, true);
                commandInfo = new RedisCommandInfo(RedisCommand.GET, key);
                q.add(commandInfo, true);
                count++;
            } while(!stop);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static class Receiver extends Thread {

        private int count;

        private int i, len, maxLen;

        private SingleConsumerQueue<RedisCommandInfo> q;

        public void run() {
            do {
                RedisCommandInfo commandInfo = null;
                    commandInfo = q.remove(1000, false);
                if(commandInfo != null) {
                    count++;
                }
                else {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } while(!stop);
        }
    }

    public static void main(String [] args) throws InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();
        int i, threadCount = 1, receiverCount = 1;
        SingleConsumerQueue<RedisCommandInfo> q = new SingleConsumerQueue<RedisCommandInfo>();
        Receiver [] receivers = new Receiver[receiverCount];
        for(i = 0; i < receiverCount; i++) {
            Receiver r = new Receiver();
            r.q = q;
            receivers[i] = r;
            receivers[i].start();
        }
        TestQueue [] testThreads = new TestQueue[threadCount];
        for(i = 0; i < threadCount; i++) {
            TestQueue tq = new TestQueue();
            tq.q = q;
            tq.prefix = "k" + i + '_';
            testThreads[i] = tq;
            testThreads[i].start();
        }
        Thread.sleep(20000);
        stop = true;
        int totalSent = 0;
        for(i = 0; i < threadCount; i++) {
            testThreads[i].join();
            System.out.println("Sent Count " + i + ": " + testThreads[i].count);
            totalSent += testThreads[i].count;
        }
        int totalReceived = 0;
        int maxLen = 0;
        for(i = 0; i < receiverCount; i++) {
            receivers[i].join();
            System.out.println("Received Count " + i + ": " + receivers[i].count);
            totalReceived += receivers[i].count;
            if(receivers[i].maxLen > maxLen) {
                maxLen = receivers[i].maxLen;
            }
        }
        long diff = System.currentTimeMillis() - start;
        System.out.println("Time = " + diff + ", total sent count = " + totalSent + " receivedCount = " + totalReceived + " maxLen = " + maxLen);
    }
}
