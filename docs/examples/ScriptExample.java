import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;
import org.aredis.cache.Script;


public class ScriptExample {

    public static void main(String [] args) throws Exception {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        // The below call returns the same AsyncRedisClient for all calls to the same server
        System.out.println("SHA1 = " + Script.ADD_SCRIPT.getSha1sum());
        AsyncRedisClient aredis = f.getClient("localhost");
        String result = (String) aredis.submitCommand(new RedisCommandInfo(RedisCommand.EVALCHECK, Script.ADD_SCRIPT, "1", "hello", "300", "world")).get().getResult();
        System.out.println("Got result: " + result);
        result = (String) aredis.submitCommand(new RedisCommandInfo(RedisCommand.EVALCHECK, Script.ADD_SCRIPT, "1", "hello", "3000", "world1")).get().getResult();
        System.out.println("Got 2nd result: " + result);
    }
}
