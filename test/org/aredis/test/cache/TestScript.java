package org.aredis.test.cache;

import java.util.concurrent.Future;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;
import org.aredis.cache.Script;


public class TestScript {

    // Below is for performance test of the simple ADD_SCRIPT with pipelined commands 2000 at a time
    public static void main(String [] args) throws Exception {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        // The below call returns the same AsyncRedisClient for all calls to the same server
        AsyncRedisClient aredis = f.getClient("my.redis.server2");
        int i, j, count = 0, pipelineSize = 2000, numCommands = 1000000, successCount = 0;
        long start = System.currentTimeMillis();

        Future<RedisCommandInfo> [] futures = new Future[pipelineSize];
        do {
            for (i = 0; i < pipelineSize; i++) {
                futures[i] = aredis.submitCommand(new RedisCommandInfo(RedisCommand.EVALCHECK, Script.ADD_SCRIPT, "1", "hello", "2", "world"));
                count++;
                if (count >= numCommands) {
                    break;
                }
            }
            for (j = 0; j < i; j++) {
                String result = (String) futures[j].get().getResult();
                if ("1".equals(result)) {
                    successCount++;
                } else if (!"0".equals(result)) {
                    System.out.println("Invalid Result: " + result);
                }
            }
        } while (count < numCommands);

        long diff = System.currentTimeMillis() - start;
        System.out.println("numCommands = " + numCommands + " time = " + diff + " rps = " + numCommands * 1000 / diff + " successful Locks = " + successCount);
    }

    // Below is the performance test of ADD_SCRIPT as main1. But the EVALSHA command is used with the sha1
    // digest instead of the script object. This is to compare with main1 which checks the script flags
    // whereas this does not access the Script Flags. The performance of main1 with the check is found to
    // be close to the below main2 (Within 98% on an average).
    public static void main2(String [] args) throws Exception {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        // The below call returns the same AsyncRedisClient for all calls to the same server
        AsyncRedisClient aredis = f.getClient("my.redis.server2");
        int i, j, count = 0, pipelineSize = 2000, numCommands = 1000000, successCount = 0;
        long start = System.currentTimeMillis();

        Future<RedisCommandInfo> [] futures = new Future[pipelineSize];
        String addScriptSha1 = Script.ADD_SCRIPT.getSha1sum();
        do {
            for (i = 0; i < pipelineSize; i++) {
                futures[i] = aredis.submitCommand(new RedisCommandInfo(RedisCommand.EVALSHA, addScriptSha1, "1", "hello", "2", "world"));
                count++;
                if (count >= numCommands) {
                    break;
                }
            }
            for (j = 0; j < i; j++) {
                String result = (String) futures[j].get().getResult();
                if ("1".equals(result)) {
                    successCount++;
                } else if (!"0".equals(result)) {
                    System.out.println("Invalid Result: " + result);
                }
            }
        } while (count < numCommands);

        long diff = System.currentTimeMillis() - start;
        System.out.println("numCommands = " + numCommands + " time = " + diff + " rps = " + numCommands * 1000 / diff + " successful Locks = " + successCount);
    }

    // The below program tests handling of multiple scripts
    public static void main3(String [] args) throws Exception {
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
