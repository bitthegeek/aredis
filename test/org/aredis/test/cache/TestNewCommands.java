package org.aredis.test.cache;

import java.util.concurrent.Future;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;


public class TestNewCommands {

    public static void main(String [] args) throws Exception {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        // Add a /etc/hosts entry for below server
        AsyncRedisClient aredis = f.getClient("my.redis.server2");
        int i;
        Future<RedisCommandInfo> futureResult = null;
        for (i = 0; i < 25; i++) {
            if (i < 24) {
                aredis.sendCommand(RedisCommand.SETEX, "k_" + i, "120", "x" + i);
            } else {
                futureResult = aredis.submitCommand(RedisCommand.SETEX, "k_" + i, "300", "x" + i);
            }
        }
        futureResult.get();
        String nextCursor = "0";
        do {
            futureResult = aredis.submitCommand(RedisCommand.SCAN, nextCursor, "MATCH", "k_*");
            Object [] results = (Object []) futureResult.get().getResult();
            nextCursor = (String) results[0];
            System.out.println("Got Next Cursor: " + nextCursor);
            Object [] matches = (Object[]) results[1];
            if (matches == null) {
                System.out.println("UNEXPECTED. Got NULL matches");
            } else {
                System.out.println("MATCHES:");
                for (i = 0; i < matches.length; i++) {
                    System.out.println((String) matches[i]);
                }
            }
        } while (!"0".equals(nextCursor));
    }
}
