import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.Script;


/*
 * This example shows how Lua scripts can be run using the EVALCHECK command which automatically ensure that
 * the script is loaded.
 */
public class ScriptExample {

    // Lua scripts should be created as static Script objects using the Script.getInstance call
    public static final Script SIMPLE_SCRIPT = Script.getInstance("return 10");

    public static void main(String [] args) throws Exception {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        AsyncRedisClient aredis = f.getClient("localhost");
        // EVALCHECK is an aredis pseudo command which translates to an EVALSHA call after verification on
        // the first call.
        String result = (String) aredis.submitCommand(RedisCommand.EVALCHECK, SIMPLE_SCRIPT, "0").get().getResult();
        System.out.println("Result of SIMPLE_SCRIPT: " + result);
        String key = "hello";
        System.out.println("SHA1 = " + Script.ADD_SCRIPT.getSha1sum());
        // The ADD_SCRIPT Lua Script is similar to the SETNX command with an expiry time specified. It will
        // return 1 on the first call since the key hello is not there.
        result = (String) aredis.submitCommand(RedisCommand.EVALCHECK, Script.ADD_SCRIPT, "1", key, "300", "world").get().getResult();
        System.out.println("Got result: " + result);
        // The second call will return 0 since the key is already present
        result = (String) aredis.submitCommand(RedisCommand.EVALCHECK, Script.ADD_SCRIPT, "1", key, "3000", "world1").get().getResult();
        System.out.println("Got 2nd result: " + result);
    }
}
