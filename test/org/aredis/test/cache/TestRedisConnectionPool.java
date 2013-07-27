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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.cache.AsyncHandler;
import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisConnection;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;
import org.aredis.cache.RedisCommandInfo.CommandStatus;
import org.aredis.util.pool.AsyncObjectPool;
import org.aredis.util.pool.AvailabeCallback;

public class TestRedisConnectionPool extends Thread {

    private static final Log log = LogFactory.getLog(TestRedisConnectionPool.class);

    private static final String TEST_KEY = "pool_test";

    private class PoolCallback implements AvailabeCallback<AsyncRedisConnection> {

        @Override
        public void use(AsyncRedisConnection con) throws Exception {
            boolean isDebug = log.isDebugEnabled();
            if(isDebug) {
                log.debug("GOT CONNECTION pos = " + pos + " con = " + con);
            }
            msg = "GOT CONNECTION";
            borrowedConnection = con;
            if(changeDb && (pos & 1) != 0) {
                con.submitCommand(new RedisCommandInfo(RedisCommand.SELECT, "3"));
            }
            RedisCommandInfo[] commands = new RedisCommandInfo[] {
                    new RedisCommandInfo(RedisCommand.WATCH, TEST_KEY),
                    new RedisCommandInfo(RedisCommand.GET, TEST_KEY)
                 };
            if(r.nextBoolean()) {
                msg = "SYNC GET CMD";
                RedisCommandInfo[] results = con.submitCommands(commands).get();
                msg = "SYNC GET CMD DONE";
                watchCallback.completed(results, null);
            }
            else {
                msg = "ASYNC GET CMD";
                con.submitCommands(commands, watchCallback);
            }
        }

    }

    private class WatchCallback implements AsyncHandler<RedisCommandInfo[]> {

        @Override
        public void completed(RedisCommandInfo results[], Throwable e) {
            boolean isDebug = log.isDebugEnabled();
            AsyncRedisConnection con = borrowedConnection;
            borrowedConnection = null;
            try {
                if(e == null && results[1].getRunStatus() == CommandStatus.SUCCESS) {
                    String s = (String) results[1].getResult();
                    int i, len = s.length();
                    int start = 0, end = s.indexOf(',');
                    if(end < 0) {
                        end = len;
                    }
                    if(isDebug) {
                        log.debug("pos = " + pos + " s = " + s + " end = " + end);
                    }
                    for(i = 0; i < pos; i++) {
                        start = end + 1;
                        end = s.indexOf(',', start);
                        if(end < 0) {
                            end = len;
                        }
                    }
                    if(isDebug) {
                        log.debug("start = " + start + " end = " + end);
                    }
                    i = Integer.parseInt(s.substring(start, end)) + 1;
                    s = s.substring(0, start) + i + s.substring(end);
                    RedisCommandInfo[] commands = new RedisCommandInfo[] {
                            new RedisCommandInfo(RedisCommand.MULTI),
                            new RedisCommandInfo(RedisCommand.SETEX, TEST_KEY, 3600, s),
                            new RedisCommandInfo(RedisCommand.EXEC)
                        };
                    if(r.nextBoolean()) {
                        msg = "SYNC MULTI CMD";
                        Future<RedisCommandInfo[]> multiResultsFuture = con.submitCommands(commands);
                        if(isDebug) {
                            log.debug("RETURNING Pos = " + pos + " con = " + con);
                        }
                        borrowMsg = "RETURN1 " + con;
                        pool.returnToPool(con);
                        RedisCommandInfo[] multiResults = multiResultsFuture.get();
                        multiCallback.completed(multiResults, null);
                    }
                    else {
                        msg = "ASYNC MULTI CMD";
                        con.submitCommands(commands, multiCallback);
                        if(isDebug) {
                            log.debug("RETURNING1 Pos = " + pos + " con = " + con);
                        }
                        borrowMsg = "RETURN2 " + con;
                        pool.returnToPool(con);
                    }
                }
                else {
                    if(isDebug) {
                        log.debug("RETURNING2 Pos = " + pos + " con = " + con);
                    }
                    borrowMsg = "RETURN3 " + con;
                    pool.returnToPool(con);
                    nextIncrement();
                }
            } catch (Exception ex) {
                borrowMsg = "RETURN4 " + con;
                pool.returnToPool(con);
                ex.printStackTrace();
            }
        }

    }

    private class MultiCallback implements AsyncHandler<RedisCommandInfo[]> {

        @Override
        public void completed(RedisCommandInfo[] results, Throwable e) {
            if(e == null && results[2].getRunStatus() == CommandStatus.SUCCESS && results[2].getResult() != null) {
                index++;
            }
            tries++;
            msg = "MULTICALLBACK";
            nextIncrement();
        }

    }

    private int index;

    private int pos;

    private int count;

    private int tries;

    private AsyncObjectPool<AsyncRedisConnection> pool;

    private AsyncRedisConnection borrowedConnection;

    private Random r;

    private CountDownLatch latch;

    private PoolCallback poolCallback;

    private WatchCallback watchCallback;

    private MultiCallback multiCallback;

    private volatile String msg;

    private volatile String borrowMsg;

    public TestRedisConnectionPool(AsyncObjectPool<AsyncRedisConnection> ppool, int ppos, int pcount, CountDownLatch platch) {
        pool = ppool;
        pos = ppos;
        count = pcount;
        r = new Random(10);
        latch = platch;
        poolCallback = new PoolCallback();
        watchCallback = new WatchCallback();
        multiCallback = new MultiCallback();
    }

    public void nextIncrement() {
        boolean isDebug = log.isDebugEnabled();
        msg = "nextIncrement";
        if(index < count) {
            AsyncRedisConnection con;
            if(r.nextBoolean()) {
                if(isDebug) {
                    log.debug("Sync Borrow pos = " + pos);
                }
                borrowMsg = "Sync Borrow";
                con = pool.syncBorrow(0);
                borrowMsg = "Sync Borrow Done " + con;
            }
            else {
                if(isDebug) {
                    log.debug("ASync Borrow pos = " + pos);
                }
                borrowMsg = "ASync Borrow";
                con = pool.asyncBorrow(poolCallback);
            }
            if(con != null) {
                try {
                    poolCallback.use(con);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        else {
            latch.countDown();
            msg = "CountDown = " + latch.getCount();
            if(isDebug) {
                log.debug("Test: " + pos + " done. latch = " + latch.getCount());
            }
        }
    }

    @Override
    public void run() {
        nextIncrement();
    }

    private static boolean changeDb = false;

    public static void main(String args[]) throws Exception {
        int i, numThreads = 12;
        int numTasks = numThreads;
        int poolSize = 4;
        int count = 300;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads, numThreads, 15, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        executor.allowCoreThreadTimeOut(true);
        AsyncRedisFactory f = new AsyncRedisFactory(executor);
        f.setPoolSize(poolSize);
        String host = "localhost/1";
        AsyncObjectPool<AsyncRedisConnection> aredisPool = f.getConnectionPool(host);
        AsyncRedisClient con = f.getClient(host);
        StringBuilder sb = new StringBuilder();
        for(i = 0; i < numTasks; i++) {
            if(i > 0) {
                sb.append(',');
            }
            sb.append(0);
        }
        String s = sb.toString();
        con.submitCommand(new RedisCommandInfo(RedisCommand.SETEX, TEST_KEY, "3600", s)).get();
        if(changeDb) {
            con.submitCommands(new RedisCommandInfo [] {
                    new RedisCommandInfo(RedisCommand.SELECT, "3"),
                    new RedisCommandInfo(RedisCommand.SETEX, TEST_KEY, "3600", s)
                    }).get()[1].getResult();
        }
        TestRedisConnectionPool tests[] = new TestRedisConnectionPool[numTasks];
        CountDownLatch l = new CountDownLatch(numTasks);
        for(i = 0; i < tests.length; i++) {
            tests[i] = new TestRedisConnectionPool(aredisPool, i, count, l);
            tests[i].start();
        }
        l.await(120, TimeUnit.SECONDS);
        System.out.println("Finished Wait");
        String val = (String) con.submitCommand(new RedisCommandInfo(RedisCommand.GET, TEST_KEY)).get().getResult();
        System.out.println("Got final value: " + val);
        if(changeDb) {
            val = (String) con.submitCommands(new RedisCommandInfo [] {
                    new RedisCommandInfo(RedisCommand.SELECT, "3"),
                    new RedisCommandInfo(RedisCommand.GET, TEST_KEY)
                    }).get()[1].getResult();
            System.out.println("Got final value on DB:3: " + val);
        }
        System.out.println("Num Tries:");
        for(i = 0; i < tests.length; i++) {
            System.out.println("" + i + ": " + tests[i].tries + " index: " + tests[i].index);
        }
        System.out.println("Messages:");
        for(i = 0; i < tests.length; i++) {
            System.out.println("" + i + ": " + tests[i].msg + " (" + tests[i].borrowMsg + ')');
        }
        aredisPool.printStatus();
        System.out.flush();
    }

}
