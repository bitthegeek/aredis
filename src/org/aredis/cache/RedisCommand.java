/*
 * Copyright (C) 2013-2014 Suresh Mahalingam.  All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE FOR
 *  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

package org.aredis.cache;

import java.util.Arrays;

import org.aredis.cache.RedisCommandInfo.ResultType;

/**
 * Enumerates the Redis Commands and also contains information about the command argument types using which the AsyncRedisConnection
 * determines the arguments to which to used the Data Handler.
 * @author Suresh
 *
 */
public enum RedisCommand {
    APPEND("kp"),
    AUTH("p"),
    BGREWRITEAOF(""),
    BGSAVE(""),
    BITCOUNT("kpp"),
    BITOP("pkk@k"),
    BITPOS("kp@p"),
    BLPOP("kk@k", false, true), // Not Accurate since there should be a p at end
    BRPOP("kk@k", false, true), // Not Accurate since there should be a p at end
    BRPOPLPUSH("kkp", false, true),
    CLIENT("p@p"),
    CONFIG("p@p"),
    DBSIZE(""),
    DECR("k"),
    DECRBY("kp"),
    DEL("k@k", false, false, IntegerShardedResultHandler.instance),
    DISCARD(""),
    DUMP("k"),
    ECHO("p"),
    EVAL("pck@v"),
    EVALCHECK("pck@v"),
    EVALSHA("pck@v"),
    /**
     * EVALCHECK is a pseudo command translating into an EVAL for the first call and then into an
     * EVALSHA since the script would have been loaded. The first parameter should be a
     * {@link Script} object. This command is useful only for scripts that are going to be run
     * multiple times which is normally the case.
     */
    EXEC("", true, false),
    EXISTS("k"),
    EXPIRE("kp"),
    EXPIREAT("kp"),
    FLUSHALL(""),
    FLUSHDB(""),
    GET("k"),
    GETBIT("kp"),
    GETRANGE("kpp"),
    GETSET("kv"),
    HDEL("kp@p"),
    HEXISTS("kp"),
    HGET("kp"),
    HGETALL("k"),
    HINCRBY("kpp"),
    HINCRBYFLOAT("kpp"),
    HKEYS("k"),
    HLEN("k"),
    HMGET("kp@p"),
    HMSET("kpv@pv"),
    HSCAN("kp@p"),
    HSET("kpv"),
    HSETNX("kpv"),
    HVALS("k"),
    INCR("k"),
    INCRBY("kp"),
    INCRBYFLOAT("kp"),
    INFO("p"),
    KEYS("p"),
    LASTSAVE(""),
    LINDEX("kp"),
    LINSERT("kppv"),
    LLEN("k"),
    LPOP("k"),
    LPUSH("kv@v"),
    LPUSHX("kv"),
    LRANGE("kpp"),
    LREM("kpv"),
    LSET("kpv"),
    LTRIM("kpp"),
    MGET("k@k", false, false, ListShardedResultHandler.instance),
    MIGRATE("ppkpp", false, true),
    MOVE("kp"),
    MSET("kv@kv", false, false, SimpleShardedResultHandler.instance),
    MSETNX("kv@kv"),
    MULTI("", true, false),
    OBJECT("pk"),
    PERSIST("k"),
    PEXPIRE("kp"),
    PEXPIREAT("kp"),
    PFADD("kp@p"),
    PFCOUNT("k@k"),
    PFMERGE("kk@k"),
    PING(""),
    PSETEX("kpv"),
    PSUBSCRIBE("p@p", true, true),
    PTTL("k"),
    PUBLISH("pv"),
    PUBSUB("p@p"),
    PUNSUBSCRIBE("@p", true, true),
    RANDOMKEY(""),
    RENAME("kk"),
    RENAMENX("kk"),
    RESTORE("kpp"),
    RPOP("k"),
    RPOPLPUSH("kk"),
    RPUSH("kv@v"),
    RPUSHX("kv"),
    SADD("kp@p"),
    SAVE(""),
    SCAN("p@p"),
    SCARD("k"),
    SCRIPT("p@p"),
    SDIFF("k@k"),
    SDIFFSTORE("kk@k"),
    SELECT("p", true, false),
    SET("kv"),
    SETBIT("kpp"),
    SETEX("kpv"),
    SETNX("kv"),
    SETRANGE("kpp"),
    SHUTDOWN("p", true, true),
    SINTER("k@k"),
    SINTERSTORE("kk@k"),
    SISMEMBER("kp"),
    SLAVEOF("pp"),
    SLOWLOG("p@p"),
    SMEMBERS("k"),
    SMOVE("kkp"),
    SORT("k@p"),
    SPOP("k"),
    SRANDMEMBER("kp"),
    SREM("kp@p"),
    SSCAN("kp@p"),
    STRLEN("k"),
    SUBSCRIBE("p@p", true, true),
    SUNION("k@k"),
    SUNIONSTORE("kk@k"),
    TIME(""),
    TTL("k"),
    TYPE("k"),
    UNSUBSCRIBE("@p", true, true),
    UNWATCH(""),
    WATCH("k@k", true, false),
    ZADD("kpp@p"),
    ZCARD("k"),
    ZCOUNT("kpp"),
    ZINCRBY("kpp"),
    ZINTERSTORE("kck@p"),
    ZLEXCOUNT("kpp"),
    ZRANGE("kpp@p"),
    ZRANGEBYLEX("kpp@p"),
    ZRANGEBYSCORE("kpp@p"),
    ZRANK("kp"),
    ZREM("kp@p"),
    ZREMRANGEBYLEX("kpp"),
    ZREMRANGEBYRANK("kpp"),
    ZREMRANGEBYSCORE("kpp"),
    ZREVRANGE("kpp@p"),
    ZREVRANGEBYSCORE("kpp@p"),
    ZREVRANK("kp"),
    ZSCAN("kp@p"),
    ZSCORE("kp"),
    ZUNIONSTORE("kck@p");

    static {
        EVAL.scriptCommand = true;
        EVALSHA.scriptCommand = true;
        EVALCHECK.scriptCommand = true;
        SCRIPT.scriptCommand = true;
        SDIFF.useKeyHandlerAsDefaultDataHandler = true;
        SINTER.useKeyHandlerAsDefaultDataHandler = true;
        SMEMBERS.useKeyHandlerAsDefaultDataHandler = true;
        SPOP.useKeyHandlerAsDefaultDataHandler = true;
        SRANDMEMBER.useKeyHandlerAsDefaultDataHandler = true;
        SUNION.useKeyHandlerAsDefaultDataHandler = true;
    }

    static interface ShardedResultHandler {
        void aggregateResults(RedisCommandInfo shardedCommand);
    }

    private static class ListShardedResultHandler implements ShardedResultHandler {

        private static ListShardedResultHandler instance = new ListShardedResultHandler();

        @Override
        public void aggregateResults(RedisCommandInfo shardedCommand) {
            RedisCommandInfo[] splitCommands = shardedCommand.splitCommands;
            int splitKeyIndexes[] = new int[splitCommands.length];
            Object[] params = shardedCommand.getParams();
            int i, j, numKeys = params.length;
            Object splitParams[][] = new Object[splitCommands.length][];
            for(i = 0; i < splitParams.length; i++) {
                if(splitCommands[i].resultType == ResultType.MULTIBULK) {
                    splitParams[i] = splitCommands[i].getParams();
                }
            }
            Object finalResult[] = new Object[numKeys];
            for(i = 0; i < numKeys; i++) {
                Object nextResult = null;
                String nextKey = (String) params[i];
                boolean found = false;
                for(j = 0; j < splitCommands.length; j++) {
                    Object[] nextSplitParams = splitParams[j];
                    int nextSplitKeyIndex = splitKeyIndexes[j];
                    if(nextSplitParams != null && nextSplitKeyIndex < nextSplitParams.length && nextKey.equals(nextSplitParams[nextSplitKeyIndex])) {
                        splitKeyIndexes[j]++;
                        Object splitResults[] = (Object[]) splitCommands[j].result;
                        if(splitResults != null) {
                            nextResult = splitResults[nextSplitKeyIndex];
                        }
                        found = true;
                        break;
                    }
                }
                if(!found) {
                    throw new IllegalStateException("Could not locate Key: " + nextKey + " in splitCommands");
                }
                finalResult[i] = nextResult;
            }
            shardedCommand.result = finalResult;
        }

    }

    private static class IntegerShardedResultHandler implements ShardedResultHandler {

        private static IntegerShardedResultHandler instance = new IntegerShardedResultHandler();

        @Override
        public void aggregateResults(RedisCommandInfo shardedCommand) {
            RedisCommandInfo[] splitCommands = shardedCommand.splitCommands;
            int i, total = 0;
            boolean foundResult = false;
            for(i = 0; i < splitCommands.length; i++) {
                Object nextResult = splitCommands[i].result;
                if(nextResult != null) {
                    total += Integer.parseInt((String) nextResult);
                    foundResult = true;
                }
            }
            if(foundResult) {
                shardedCommand.result = String.valueOf(total);
            }
        }

    }

    private static class SimpleShardedResultHandler implements ShardedResultHandler {

        private static SimpleShardedResultHandler instance = new SimpleShardedResultHandler();

        @Override
        public void aggregateResults(RedisCommandInfo shardedCommand) {
            RedisCommandInfo[] splitCommands = shardedCommand.splitCommands;
            int i;
            for(i = 0; i < splitCommands.length; i++) {
                Object nextResult = splitCommands[i].result;
                if(nextResult != null) {
                    shardedCommand.result = nextResult;
                    break;
                }
            }
        }

    }

    private static class DefaultShardedResultHandler implements ShardedResultHandler {

        private static DefaultShardedResultHandler instance = new DefaultShardedResultHandler();

        @Override
        public void aggregateResults(RedisCommandInfo shardedCommand) {
            switch(shardedCommand.resultType) {
            case MULTIBULK:
                ListShardedResultHandler.instance.aggregateResults(shardedCommand);
                break;
            case INT:
                IntegerShardedResultHandler.instance.aggregateResults(shardedCommand);
                break;
            default:
                SimpleShardedResultHandler.instance.aggregateResults(shardedCommand);
                break;
            }
        }

    }

    // argInfo comprises of 0 or more characters indicating expected arg type.
    // k: key, v: value, c: count indicator for next argument for zrangebyscore, p: Other command Parameter.
    // Anything after an @ (Only one makes sense) is repeatable zero or more times.
    private RedisCommand(String argInfo) {
        int len = argInfo.length();
        int numArgTypes = len;
        if(argInfo.indexOf('@') >= 0) {
            numArgTypes--;
        }
        argTypes = new char[numArgTypes];
        int i, argTypeIndex = 0, numKeys = 0;
        repeatableFromIndex = -1;
        for(i = 0; i < len; i++) {
            char ch = argInfo.charAt(i);
            if(ch == '@') {
                if(repeatableFromIndex >= 0) {
                    throw new IllegalArgumentException("More than one @ in argInfo: " + argInfo + " for RedisCommand: " + name());
                }
                repeatableFromIndex = argTypeIndex;
            }
            else {
                if(ch == 'k') {
                    if(repeatableFromIndex >= 0) {
                        numKeys = 100;
                    }
                    else {
                        numKeys++;
                    }
                }
                else if(ch != 'v' && ch != 'p' && ch != 'c') {
                    throw new IllegalArgumentException("Illegal char: " + ch + " in argInfo: " + argInfo + " for RedisCommand: " + name());
                }
                argTypes[argTypeIndex] = ch;
                argTypeIndex++;
            }
        }
        if(numKeys == 1) {
            shardedResultHandler = DefaultShardedResultHandler.instance;
        }
        if (argInfo.indexOf('k') < 0) {
            useKeyHandlerAsDefaultDataHandler = true;
        }
        if (name().startsWith("Z")) {
            useKeyHandlerAsDefaultDataHandler = true;
        }
        if (name().indexOf("SCAN") >= 0) {
            useKeyHandlerAsDefaultDataHandler = true;
        }
    }

    private RedisCommand(String argInfo, boolean pstateful, boolean pblocking) {
        this(argInfo);
        stateful = pstateful;
        blocking = pblocking;
    }

    private RedisCommand(String argInfo, boolean pstateful, boolean pblocking, ShardedResultHandler pshardedResultHandler) {
        this(argInfo, pstateful, pblocking);
        shardedResultHandler = pshardedResultHandler;
    }

    char argTypes[];

    private int repeatableFromIndex;

    private boolean useKeyHandlerAsDefaultDataHandler;

    ShardedResultHandler shardedResultHandler ;

    private boolean blocking;

    private boolean stateful;

    private boolean scriptCommand;

    /**
     * Gets the command argument types as a character array. In the array k stands for a key, v stands for a value,
     * p stands for a parameter like the expiry in SETEX command or the increment number in INCRBY command, c stands for
     * a count for which the next argType occurs for example in EVAL or ZINTERSTORE. In case the
     * same pattern repeats like in the command MSET it is captured by getRepeatable from index. These two capture the
     * syntax for most redis commands except for ones like BLPOP where we have a parameter after multiple keys
     * and EVAL and EVALSHA where all of the values after the keys are assumed to be values but some of them could be
     * parameters like numbers which should be Ok if you use a Data Handler like JavaHandler which serializes Strings
     * the natural way.
     * @return The arg types
     */
    public char[] getArgTypes() {
        char types[] = argTypes;
        if(types.length > 0) {
            types = Arrays.copyOf(types, types.length);
        }

        return types;
    }

    /**
     * Gets the repeatable from index int argTypes from which the argTypes are repeated or -1 if there is a fixed list of arguments.
     * For example for HMSET the argTypes are kpvpv and the repeatable from index is 3 to indicate that pv can be repeated.
     * @return Repeatable from Index
     */
    public int getRepeatableFromIndex() {
        return repeatableFromIndex;
    }

    /**
     * Gets if Key Handler is to be used as the default data handler for this command. This is set for commands
     * without any k in argTypes (i.e without any key arguments) and for *SCAN*, SET and SORTED SET commands
     * (Starting with Z). This is mainly intended to force Bulk responses for these commands to be treated as
     * Strings. This is anyways handled in the default data handler of AsyncRedisConnection unless it is changed.
     * @return True if Key Handler is to be used as the default handler instead of the configured
     * default handler
     */
    public boolean isUseKeyHandlerAsDefaultDataHandler() {
        return useKeyHandlerAsDefaultDataHandler;
    }

    /**
     * Identifies blocking commands like BLPOP
     * @return True if the command is blocking
     */
    public boolean isBlocking() {
        return blocking;
    }

    /**
     * Identifies if the command is stateful i.e the command has an effect on subsequent commands run on the same connections.
     * Examples are WATCH, SELECT, SUBSCRIBE.
     * @return True if the command is stateful false otherwise
     */
    public boolean isStateful() {
        return stateful;
    }

    /**
     * Identifies if the command is one of the SCRIPT commands, EVAL, EVALSHA, EVALCHECK or SCRIPT.
     *
     * @return True if the command is a script command, false otherwise
     */
    public boolean isScriptCommand() {
        return scriptCommand;
    }
}
