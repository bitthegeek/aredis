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

package org.aredis.test.util.pool;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.aredis.util.pool.AccessUtil;
import org.aredis.util.pool.AsyncObjectPool;
import org.aredis.util.pool.AsyncPoolMemberManager;
import org.aredis.util.pool.AvailabeCallback;

public class TestAsyncObjectPool extends Thread {

    private static class PoolObject {
        private volatile boolean inUse;
        private int usageCount;
        private volatile boolean inPool;
    }

    public static class PoolCallback implements AvailabeCallback<PoolObject> {

        private int seq;

        private long sleepMillis;

        private AsyncObjectPool<PoolObject> pool;

        public volatile int state;

        @Override
        public void use(PoolObject e) throws Exception {
            if(!e.inPool) {
                System.out.println("Member: " + e + " Not in pool");
            }
            if(!e.inUse) {
                e.inUse = true;
                e.usageCount++;
            }
            else {
                System.out.println("Member to Async Callback: " + e + " already in use");
            }
            if(sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }
            e.inUse = false;
            pool.returnToPool(e);
            callbacks.remove(seq);
        }

    }

    private static class PoolObjectManager implements AsyncPoolMemberManager<PoolObject> {

        private final static PoolObject globalPool[];

        static {
            int i;
            globalPool = new PoolObject[100];
            for(i = 0; i < globalPool.length; i++) {
                globalPool[i] = new PoolObject();
            }
        }

        private static PoolObject screateObject() {
            PoolObject poolObject = null;
            int i;
            for(i = 0; i < globalPool.length; i++) {
                PoolObject po = globalPool[i];
                if(!po.inPool) {
                    synchronized(po) {
                        if(!po.inPool) {
                            po.inPool = true;
                            poolObject = po;
                            break;
                        }
                    }
                }
            }
            if(poolObject == null) {
                throw new NullPointerException("Could Not Create PoolObject. globalPool FULL");
            }

            return poolObject;
        }

        @Override
        public PoolObject createObject() {
            return screateObject();
        }

        @Override
        public void destroyObject(PoolObject item) {
            item.inPool = false;
            // System.out.println("ITEM " + item + " returned");
        }

        @Override
        public void onBorrow(PoolObject item) {
        }

        @Override
        public void beforeReturn(PoolObject item) {
        }

    }

    private static volatile boolean stop;

    private static AtomicInteger seq = new AtomicInteger();

    private static Map<Integer, PoolCallback> callbacks = new ConcurrentHashMap<Integer, TestAsyncObjectPool.PoolCallback>();

    private AsyncObjectPool<PoolObject> pool;

    private int maxSleepMillis;

    private int asyncCallIntervalMillis;

    public TestAsyncObjectPool(AsyncObjectPool<PoolObject> ppool, int pmaxSleepMillis, int pasyncCallIntervalMillis) {
        pool = ppool;
        maxSleepMillis = pmaxSleepMillis;
        asyncCallIntervalMillis = pasyncCallIntervalMillis;
    }

    @Override
    public void run() {
        int i = 0;
        long startTime = System.currentTimeMillis();
        long prevAsyncTime = startTime;
        Random r = new Random();
        do {
            int typ = i % 3;
            PoolObject member = null;
            Future<PoolObject> futureMember = null;
            int sleepMillis = r.nextInt(maxSleepMillis);
            boolean isAsync = false;
            if(typ == 0 || typ == 2 && System.currentTimeMillis() - prevAsyncTime < asyncCallIntervalMillis) {
                member = pool.syncBorrow(10);
            }
            else if (typ == 1) {
                futureMember = pool.borrow(10);
                if(futureMember != null) {
                    try {
                        member = futureMember.get(15, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                    }
                }
            }
            else {
                isAsync = true;;
                prevAsyncTime = System.currentTimeMillis();
                PoolCallback callback = new PoolCallback();
                callback.seq = seq.incrementAndGet();
                callback.pool = pool;
                callback.sleepMillis = sleepMillis;
                callbacks.put(callback.seq, callback);
                member = pool.asyncBorrow(callback);
                if(member != null) {
                    callbacks.remove(callback.seq);
                    isAsync = false;
                }
            }
            if(!isAsync) {
                if(member == null) {
                    System.out.println("MEMBER = NULL typ = " + typ);
                }
                else {
                    if(!member.inPool) {
                        System.out.println("Member: " + member + " Not in pool");
                    }
                    if(!member.inUse) {
                        member.inUse = true;
                        member.usageCount++;
                    }
                    else {
                        System.out.println("Member: " + member + " typ = " + typ + " already in use");
                    }
                    if(sleepMillis > 0) {
                        try {
                            Thread.sleep(sleepMillis);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    member.inUse = false;
                    pool.returnToPool(member);
                }
            }
            i++;
        } while(!stop);
    }

    public static void main(String args[]) throws Exception {

        int i, poolSize = 20;

        Random r = new Random();

        ThreadPoolExecutor executor = new ThreadPoolExecutor(20, 20, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

        executor.allowCoreThreadTimeOut(true);

        PoolObjectManager objectManager = new PoolObjectManager();

        AsyncObjectPool<PoolObject> pool = new AsyncObjectPool<TestAsyncObjectPool.PoolObject>(objectManager , poolSize, executor);

        pool.reconcileCount = new AtomicInteger();

        int maxSleepMillis = 10;

        int asyncCallIntervalMillis = 50;

        int numThreads = 30;

        PoolObject poolObject;

        TestAsyncObjectPool tests[] = new TestAsyncObjectPool[numThreads];

        for(i = 0; i < tests.length; i++) {
            tests[i] = new TestAsyncObjectPool(pool, maxSleepMillis, asyncCallIntervalMillis);
            tests[i].start();
        }

        for(i = 0; i < 10; i++) {
            int newSize = r.nextInt(2*poolSize) + 1;
            pool.setPoolSize(newSize);
            pool.setPoolSize(newSize + 3);
            Thread.sleep(5);
            pool.setPoolSize(newSize);
            Thread.sleep(1000);
        }
        pool.setPoolSize(poolSize);
        Thread.sleep(2000);
        /*
        pool.setPoolSize(15);
        Thread.sleep(2000);
        pool.setPoolSize(8);
        Thread.sleep(2000);
        pool.setPoolSize(5);
        Thread.sleep(20000);
        */

        stop = true;

        Thread.sleep(2000);

        for(i = 0; i < tests.length; i++) {
            tests[i].join();
        }

        System.out.println("STATUS OF FINAL POOL MEMBERS:");
        PoolObject[] globalPool = PoolObjectManager.globalPool;
        for(i = 0; i < globalPool.length; i++) {
            poolObject = globalPool[i];
            if(poolObject.inPool || poolObject.usageCount > 0) {
                System.out.println("" + i + ". In Pool: " + poolObject.inPool + ". In Use: " + poolObject.inUse + " Usage Count: " + poolObject.usageCount);
            }
        }
        int pendingCallbacks = callbacks.size();
        long pollEndTime = System.currentTimeMillis() + maxSleepMillis + 2000;
        while(pendingCallbacks > 0 && System.currentTimeMillis() < pollEndTime) {
            Thread.sleep(500);
            pendingCallbacks = callbacks.size();
        }
        if(pendingCallbacks > 0) {
            System.out.println("ERROR: " + pendingCallbacks + " still pending");
            for(Integer seq : callbacks.keySet()) {
                System.out.println("KEY = " + seq + " state = " + callbacks.get(seq).state);
            }
        }
        System.out.println("Pool Size: " + pool.getPoolSize() + " QSIZE: " + AccessUtil.getWaitingUsers(pool).size());
        System.out.println("NUM RECONCILES = " + pool.reconcileCount);
    }
}
