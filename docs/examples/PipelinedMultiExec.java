import java.util.ArrayList;
import java.util.List;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;


/*
 * It is possible to run a set of MULTI-EXEC commands without WATCH.
 *
 * This ensures the atomicity of the commands, i.e either all are executed or none are executed
 * if the client or the server is shutting down or if there is an error.
 *
 * If you need MULTI-EXEC along with WATCH on one or more keys for CAS you need to use
 * a connection pool.
 *
 * The below example stores a key into a sorted set with the current timestamp as score and
 * also sets a java ArrayList as value against the key.
 */
public class PipelinedMultiExec {
    public static void main(String args[]) throws Exception {
        String mainKey = "aredis_ex_keys";
        String key = "hello";
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        // Connecting to DB index 1, default DB index is 0. You can also pass localhost:6379/1 for port
        AsyncRedisClient aredis = f.getClient("localhost/1");
        List<String> val = new ArrayList<String>(1);
        val.add("hello");
        aredis.submitCommands(new RedisCommandInfo[] {
           new RedisCommandInfo(RedisCommand.MULTI),
           new RedisCommandInfo(RedisCommand.ZADD, mainKey, String.valueOf(System.currentTimeMillis()), key),
           // Setting an expire time on master key as well since this is an example
           new RedisCommandInfo(RedisCommand.EXPIRE, mainKey, "400"),
           new RedisCommandInfo(RedisCommand.SETEX, key, "300", val),
           new RedisCommandInfo(RedisCommand.EXEC)
        }).get();
        // Verify the values
        val = (List<String>) aredis.submitCommand(RedisCommand.GET, "hello").get().getResult();
        System.out.println("Got back val = " + val);
        Object vals[] = (Object[]) aredis.submitCommand(RedisCommand.ZRANGEBYSCORE, mainKey, "0", "+inf", "WITHSCORES").get().getResult();
        System.out.println("Got back key: " + vals[0] + ". Timestamp(score) = " + vals[1]);
    }

}
