import java.util.concurrent.Future;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;

/*
 * Below is an example of Sharding. The usage is similar to a regular client. The only thing is that you give
 * a comma separated list of servers.
 *
 * The commands to a ShardedAsyncRedisClient is restricted to commands with exactly 1 key except MGET and MSET.
 */
public class ShardingExample {
    public static void main(String args[]) {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        // Normally getClient will be called with 2 or more different servers. For testing purposes we are sharding
        // within localhost server between 2 DBs. The below call returns a ShardedAsyncRedisClient rather than an
        // AsyncRedisConnection which is returned when getClient is called with a single server
        AsyncRedisClient aredis = f.getClient("localhost,localhost/1");
        // Use sendCommand instead of submitCommand when you are not interested in the Return value
        aredis.sendCommand(RedisCommand.SETEX, "hello0", "300", "world0");
        aredis.sendCommand(RedisCommand.SETEX, "hello1", "300", "world1");
        Future<RedisCommandInfo> future = aredis.submitCommand(RedisCommand.GET, "hello0");
        // Deleting the above and non-existent keys
        Future<RedisCommandInfo> future1 = aredis.submitCommand(RedisCommand.DEL, "hello0", "hello1", "hello2", "hello3", "hello4");
        // Setting the values back in case you want to use redis-cli to check which key is in which DB
        aredis.sendCommand(RedisCommand.SETEX, "hello0", "300", "world0");
        aredis.sendCommand(RedisCommand.SETEX, "hello1", "300", "world1");
        try {
            String val = (String) future.get().getResult();
            System.out.println("Got back val = " + val + " val1 = " + future1.get().getIntResult(-1));
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
