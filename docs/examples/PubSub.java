import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisMessageListener;
import org.aredis.cache.SubscriptionInfo;
import org.aredis.messaging.RedisSubscription;


/*
 * Below demonstrates the use of redis PubSub. Publish is executed as a regular command. For subscription there
 * is a separate RedisSubscription class using which you subscribe to a channel.
 */
public class PubSub {

    private static class Listener implements RedisMessageListener {

        @Override
        public void receive(SubscriptionInfo subscriptionInfo,
                String channelName, Object message) throws Exception {
            System.out.println("Channel: " + channelName + ", got message: " + message);
        }

    }

    public static void main(String args[]) throws InterruptedException {
        // Please configure the factory with an existing or new executor for Async API or Subscription.
        // It is better to always pass an exeuctor to the factory.
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 15, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        executor.allowCoreThreadTimeOut(true);
        AsyncRedisFactory f = new AsyncRedisFactory(executor);
        RedisSubscription subscription = f.getSubscription("localhost");
        Listener listener = new Listener();
        subscription.subscribe(new SubscriptionInfo("alarm", listener));
        // The below sleep is required to give time for the subscription to be sent since it is asynchronous
        Thread.sleep(100);
        AsyncRedisClient aredis = f.getClient("localhost");
        aredis.sendCommand(RedisCommand.PUBLISH, "alarm", "buzz");
    }
}
