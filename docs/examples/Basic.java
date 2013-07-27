import java.util.concurrent.Future;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;


public class Basic {
    public static void main(String args[]) {
        // In a Server create only one instance of AsyncRedisFactory
        // by configuring it as a Spring Bean or as a Singleton
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        // The below call returns the same AsyncRedisClient for all calls to the same server
        AsyncRedisClient aredis = f.getClient("localhost");
        // Use sendCommand instead of submitCommand when you are not interested in the Return value
        aredis.sendCommand(RedisCommand.SETEX, "hello", "300", "world");
        Future<RedisCommandInfo> future = aredis.submitCommand(RedisCommand.GET, "hello");
        try {
            String val = (String) future.get().getResult();
            System.out.println("Got back val = " + val);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
