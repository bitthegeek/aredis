package org.aredis.test.cache;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;
import org.aredis.cache.Script;


public class TestScript {

    public static void main(String [] args) throws Exception {
        // Create 100 different scripts by adding spaces to ADD_SCRIPT for testing
        // Better to cleanup the server by doing SCRIPT FLUSH after this test
        String referenceScript = Script.ADD_SCRIPT.getScript();
        int i, blankPos = referenceScript.indexOf(' ');
        StringBuilder sb = new StringBuilder(referenceScript);
        Script [] scripts = new Script[100];
        for (i = 0; i < scripts.length; i++) {
            sb.insert(blankPos, ' ');
            scripts[i] = Script.getInstance(sb.toString());
        }

        AsyncRedisFactory f = new AsyncRedisFactory(null);
        // The below call returns the same AsyncRedisClient for all calls to the same server
        AsyncRedisClient aredis = f.getClient("localhost");
        // Add a /etc/hosts entry for below server
        AsyncRedisClient aredis1 = f.getClient("my.redis.server1");
        long start = System.currentTimeMillis();
        do {
            for (i = 0; i < scripts.length; i++) {
                String result = (String) aredis.submitCommand(new RedisCommandInfo(RedisCommand.EVALCHECK, scripts[i], "1", "hello", "10", "world")).get().getResult();
                System.out.println("Got result: " + result);
                result = (String) aredis1.submitCommand(new RedisCommandInfo(RedisCommand.EVALCHECK, scripts[scripts.length - i - 1], "1", "hello", "10", "world1")).get().getResult();
                System.out.println("Got 2nd result: " + result);
            }
            Thread.sleep(10000);
        } while (System.currentTimeMillis() - start < 120000);
    }
}
