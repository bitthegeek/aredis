package org.aredis.cache;

import java.util.Arrays;

import org.aredis.net.AsyncSocketTransport;
import org.aredis.net.AsyncSocketTransportFactory;
import org.aredis.net.ServerInfo;

class RedisServerWideData {

    private static volatile RedisServerWideData [] instances = new RedisServerWideData[0];

    private ServerInfo serverInfo;

    private volatile AsyncRedisConnection commonAredisConnection;

    private volatile ScriptStatuses scriptStatuses;

    private RedisServerWideData(ServerInfo pserverInfo) {
        serverInfo = pserverInfo;
    }

    public static RedisServerWideData getInstance(ServerInfo serverInfo) {
        int serverIndex = serverInfo.getServerIndex();
        if (serverIndex > 100000) {
            throw new RuntimeException("ServerIndex too big, 100000 is max allowed: " + serverIndex);
        }
        RedisServerWideData d = null;
        RedisServerWideData [] instancesArray = instances;
        if (instancesArray.length > serverIndex) {
            d = instancesArray[serverIndex];
        }
        if (d == null) {
            synchronized (RedisServerWideData.class) {
                instancesArray = instances;
                int newLen = instancesArray.length;
                if (newLen > serverIndex) {
                    d = instancesArray[serverIndex];
                } else {
                    newLen = serverIndex + 1;
                }
                if (d == null) {
                    d = new RedisServerWideData(serverInfo);
                    instancesArray = Arrays.copyOf(instancesArray, newLen);
                    instancesArray[serverIndex] = d;
                    instances = instancesArray;
                }
            }
        }

        return d;
    }

    /**
     * Gets a common Redis connection for a given server. The uses of this common connection must be synchronized
     * on the aredis object. Also the dbIndex is honoured only for the first call. Currently the common connection is
     * used only for Redis Class Descriptor storage so it is fine. In future if a different dbIndex is required
     * the user of aredis should use the select command and have a select back to the original dbIndex in a finally block.
     *
     * @param tf Transport Factory to use or null  for default
     * @param dbIndex dbIndex to use
     * @return Common aredis connection one per server
     */
    public AsyncRedisConnection getCommonAredisConnection(AsyncSocketTransportFactory tf, int dbIndex) {
        AsyncRedisConnection aredis = commonAredisConnection;
        if (aredis == null) {
            synchronized(this) {
                aredis = commonAredisConnection;
                if (aredis == null) {
                    if(tf == null) {
                        tf = AsyncSocketTransportFactory.getDefault();
                    }
                    AsyncSocketTransport con = tf.getTransport(serverInfo.getHost(), serverInfo.getPort());
                    aredis = new AsyncRedisConnection(con, dbIndex, null, ConnectionType.STANDALONE);
                    aredis.isCommonConnection = true;
                    commonAredisConnection = aredis;
                }
            }
        }
        return aredis;
    }

    public ScriptStatuses getScriptStatuses() {
        ScriptStatuses statuses = scriptStatuses;
        if (statuses == null) {
            synchronized(this) {
                statuses = scriptStatuses;
                if (statuses == null) {
                    statuses = new ScriptStatuses();
                    scriptStatuses = statuses;
                }
            }
        }
        return statuses;
    }
}
