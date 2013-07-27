import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;


/*
 * Below is an example of using multiple DB's with the same pipelined connection by sending an array of CommandInfos
 * with a SELECT command.
 *
 * The standard way to access different DB's is of to create separate clients. But you can use the below
 * if you want to access another DB occasionally.
 */
public class SwitchDB {
    public static void main(String args[]) throws InterruptedException, ExecutionException {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        AsyncRedisClient aredis = f.getClient("localhost");
        // set hello as world to DB 0
        aredis.sendCommand(RedisCommand.SETEX, "hello", "300", "world");
        Future<RedisCommandInfo[]> futures = aredis.submitCommands(new RedisCommandInfo[] {
            new RedisCommandInfo(RedisCommand.SELECT, "1"),
            new RedisCommandInfo(RedisCommand.SETEX, "hello", "300", "world1"),
            new RedisCommandInfo(RedisCommand.GET, "hello")
        });
        // The DB is automatically restored to the connection default of 0 by inserting SELECT 0 command
        // before the below is executed.
        Future<RedisCommandInfo> future = aredis.submitCommand(RedisCommand.GET, "hello");
        String val = (String) future.get().getResult();
        String val1 = (String) futures.get()[2].getResult();
        System.out.println("Got back val = " + val + " val1 = " + val1);
    }
}
