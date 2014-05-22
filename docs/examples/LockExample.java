import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;
import org.aredis.cache.Script;
import org.aredis.cache.RedisCommandInfo.CommandStatus;


/*
 * This Example shows how the ADD_SCRIPT can be used to easily implement a distributed lock with an expiry
 * that acquires a lock against a key on a redis server.
 *
 * Post Redis 2.6.12 the ADD_SCRIPT is not required. Look at the second example to see how the same can be
 * achieved using the SET command.
 */
public class LockExample {

    // Check the next example of the same implementation using the enhanced SET command
    public static void main1(String [] args) throws Exception {
        String keyToLock = "locked";
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        AsyncRedisClient aredis = f.getClient("localhost");
        // Below command tries to acquire a lock using the "locked" key valid for 5 seconds and is expected
        // to succeed
        String result = (String) aredis.submitCommand(RedisCommand.EVALCHECK, Script.ADD_SCRIPT, "1", keyToLock, "5", "X").get().getResult();
        System.out.print("Result of 1st Lock attempt: "); // SUCCESS is the expected response
        if ("1".equals(result)) {
            System.out.println("SUCCESS");
        } else if ("0".equals(result)) {
            System.out.println("FAILURE");
        } else {
            // Note that the below UNKNOWN outcome of a lock attempt is also possible if the Redis server is
            // down or not reachable in which case result is null
            System.out.println("UNKNOWN");
        }

        // The 2nd attempt to acquire the lock is expected to fail since the lock is acquired in the first call
        result = (String) aredis.submitCommand(RedisCommand.EVALCHECK, Script.ADD_SCRIPT, "1", keyToLock, "5", "X").get().getResult();
        System.out.print("Result of 2nd Lock attempt: "); // FAILURE is the expected response
        if ("1".equals(result)) {
            System.out.println("SUCCESS");
        } else if ("0".equals(result)) {
            System.out.println("FAILURE");
        } else {
            System.out.println("UNKNOWN");
        }

        Thread.sleep(6000);

        // The 3rd attempt to acquire the lock after 6 seconds is expected to succeed since the lock acquired
        // in the 1st attempt expires in 5 seconds
        result = (String) aredis.submitCommand(RedisCommand.EVALCHECK, Script.ADD_SCRIPT, "1", keyToLock, "5", "X").get().getResult();
        System.out.print("Result of 3rd Lock attempt: "); // SUCCESS is the expected response
        if ("1".equals(result)) {
            System.out.println("SUCCESS");
        } else if ("0".equals(result)) {
            System.out.println("FAILURE");
        } else {
            System.out.println("UNKNOWN");
        }

        // To release the lock instead of letting it expire just delete the key
        aredis.submitCommand(RedisCommand.DEL, keyToLock);
    }

    public static void main(String [] args) throws Exception {
        String keyToLock = "locked";
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        AsyncRedisClient aredis = f.getClient("localhost");
        // Below command tries to acquire a lock using the "locked" key valid for 5 seconds and is expected
        // to succeed
        RedisCommandInfo commandInfo = aredis.submitCommand(RedisCommand.SET, keyToLock, "X", "EX", 5, "NX").get();
        String result = (String) commandInfo.getResult();
        System.out.print("Result of 1st Lock attempt: "); // SUCCESS is the expected response
        if ("OK".equals(result)) {
            System.out.println("SUCCESS");
        } else if (commandInfo.getRunStatus() == CommandStatus.SUCCESS) {
            System.out.println("FAILURE");
        } else {
            // Note that the below UNKNOWN outcome of a lock attempt is also possible if the Redis server is
            // down or not reachable in which case result is null
            System.out.println("UNKNOWN");
        }

        // The 2nd attempt to acquire the lock is expected to fail since the lock is acquired in the first call
        commandInfo = aredis.submitCommand(RedisCommand.SET, keyToLock, "X", "EX", "5", "NX").get();
        result = (String) commandInfo.getResult();
        System.out.print("Result of 2nd Lock attempt: "); // FAILURE is the expected response
        if ("OK".equals(result)) {
            System.out.println("SUCCESS");
        } else if (commandInfo.getRunStatus() == CommandStatus.SUCCESS) {
            System.out.println("FAILURE");
        } else {
            System.out.println("UNKNOWN");
        }

        Thread.sleep(6000);

        // The 3rd attempt to acquire the lock after 6 seconds is expected to succeed since the lock acquired
        // in the 1st attempt expires in 5 seconds
        commandInfo = aredis.submitCommand(RedisCommand.SET, keyToLock, "X", "EX", "5", "NX").get();
        result = (String) commandInfo.getResult();
        System.out.print("Result of 3rd Lock attempt: "); // SUCCESS is the expected response
        if ("OK".equals(result)) {
            System.out.println("SUCCESS");
        } else if (commandInfo.getRunStatus() == CommandStatus.SUCCESS) {
            System.out.println("FAILURE");
        } else {
            System.out.println("UNKNOWN");
        }

        // To release the lock instead of letting it expire just delete the key
        aredis.submitCommand(RedisCommand.DEL, keyToLock);
    }
}
