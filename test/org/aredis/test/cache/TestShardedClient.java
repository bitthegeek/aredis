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

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;

public class TestShardedClient {

    public static final String KEY_PREFIX = "key";

    public static void main(String args[]) throws Exception {
        String key;
        int i, j;
        int count = 10;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 15, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        executor.allowCoreThreadTimeOut(true);
        AsyncRedisFactory f = new AsyncRedisFactory(executor);
        String host = "localhost,10.10.10.10,10.10.10.11";
        Future<RedisCommandInfo> futureResult = null;
        AsyncRedisClient client = f.getClient(host);
        Object keys[] = new Object[count];
        for(i = 0; i < count; i++) {
            key = KEY_PREFIX + i;
            keys[i] = key;
        }
        Object msetParams[] = new Object[2*count];
        for(i = 0, j = 0; i < count; i++) {
            key = (String) keys[i];
            msetParams[j] = key;
            j++;
            msetParams[j] = String.valueOf(i + 1);
            j++;
        }
        client.submitCommand(new RedisCommandInfo(RedisCommand.MSET, msetParams)).get().getResult();
        for(i = 0; i < count; i++) {
            key = (String) keys[i];
            client.submitCommand(new RedisCommandInfo(RedisCommand.DECRBY, key, "3"));
            client.submitCommand(new RedisCommandInfo(RedisCommand.INCRBY, key, "2"));
            String val = (String) client.submitCommand(new RedisCommandInfo(RedisCommand.GET, key)).get().getResult();
            System.out.println("Got val = " + val);
            if(!String.valueOf(i).equals(val)) {
                System.out.println("ERROR Got MSET " + val + " instead of " + i);
            }
        }
        for(i = 0; i < count; i++) {
            key = (String) keys[i];
            client.submitCommand(new RedisCommandInfo(RedisCommand.SETEX, key, 3600, key));
        }
        for(i = 0; i < count; i++) {
            key = (String) keys[i];
            String val = (String) client.submitCommand(new RedisCommandInfo(RedisCommand.GET, key)).get().getResult();
            if(!key.equals(val)) {
                System.out.println("ERROR Got " + val + " instead of " + key);
            }
        }
        Object results[] = (Object[]) client.submitCommand(new RedisCommandInfo(RedisCommand.MGET, keys)).get().getResult();
        for(i = 0; i < count; i++) {
            key = (String) keys[i];
            String val = (String) results[i];
            if(!key.equals(val)) {
                System.out.println("ERROR Got MGET " + val + " instead of " + key);
            }
        }
    }
}
