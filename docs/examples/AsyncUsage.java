import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.aredis.cache.AsyncHandler;
import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;


/*
 * The below example shows Basic Asynchronous Usage of the Aredis API.
 */
public class AsyncUsage {
    private static class CompletionHandler implements AsyncHandler<RedisCommandInfo> {

        @Override
        public void completed(RedisCommandInfo result, Throwable e) {
            if(e == null) {
                System.out.println("Got Result: " + result.getResult());
            }
            else {
                e.printStackTrace();
            }
        }

    }

    public static void main(String args[]) throws InterruptedException {
        // Please configure the factory with an existing or new executor for Async API or Subscription
        // It is better to always pass an exeuctor to the factory.
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 15, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        executor.allowCoreThreadTimeOut(true);
        AsyncRedisFactory f = new AsyncRedisFactory(executor);
        // Get a client to DB 1 instead of the default of 0
        AsyncRedisClient aredis = f.getClient("localhost/1");
        aredis.sendCommand(RedisCommand.SETEX, "hello", "300", "world");
        CompletionHandler handler = new CompletionHandler();
        aredis.submitCommand(new RedisCommandInfo(RedisCommand.GET, "hello"), handler);
    }

}
