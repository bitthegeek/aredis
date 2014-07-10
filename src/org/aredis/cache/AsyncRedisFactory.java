/*
 * Copyright (C) 2013 Suresh Mahalingam.  All rights reserved.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.messaging.RedisSubscription;
import org.aredis.net.AsyncSocketTransport;
import org.aredis.net.AsyncSocketTransportFactory;
import org.aredis.util.DefaultRedisTimer;
import org.aredis.util.RedisTimer;
import org.aredis.util.pool.AsyncObjectPool;
import org.aredis.util.pool.LeakCheckingAsyncObjectPool;

/**
 * This is the preferred class to use to get aredis Service Objects like {@link AsyncRedisConnection},
 * {@link AsyncObjectPool}<AsyncRedisConnection> and {@link RedisSubscription}. In a server environment it should
 * be configured as a Singleton or as a Spring Bean. It returns the Same Service Object on Subsequent calls for the
 * same server or comma separated list of servers. Currently the AsyncRedisConnection there is no way to free
 * AsyncRedisConnection Objects that are no longer needed. But the connections close after the configured idle timeout.
 * @author Suresh
 *
 */
public class AsyncRedisFactory {

    private static enum ServiceType {CONNECTION, CONNECTIONPOOL, SUBSCRIPTION};

    private class AsyncRedisService implements Cloneable {
        private AsyncRedisConnection connection;

        private AsyncObjectPool<AsyncRedisConnection> connectionPool;

        private RedisSubscription subscription;

        public AsyncRedisService clone() {
            AsyncRedisService clonedObject = null;
            try {
                clonedObject = (AsyncRedisService) super.clone();
            } catch (CloneNotSupportedException e) {
            }
            return clonedObject;
        }
    }

    private static volatile RedisTimer timer;

    private static final Log log = LogFactory.getLog(AsyncRedisFactory.class);

    private AsyncSocketTransportFactory transportFactory;

    private AsyncSocketTransportFactory poolTransportFactory;

    private AsyncSocketTransportFactory subscriptionTransportFactory;

    private Executor executor;

    private int poolSize;

    private int shutdownDelay;

    private volatile Map<String, AsyncRedisService> serviceMap;

    private volatile Map<String, ShardedAsyncRedisClient> shardedConnectionMap;

    private DataHandler dataHandler;

    /**
     * Creates an AsyncRedisFactory Object.
     * @param ptransportFactory Connection Factory to use to create {@link AsyncSocketTransport}
     * @param pexecutor Common Executor to use for aredis services
     * @param ppoolSize Pool Size when creating a connection pool
     * @param pshutdownDelay Shutdown delay in seconds when shutting down. This is currently not used.
     */
    public AsyncRedisFactory(AsyncSocketTransportFactory ptransportFactory, Executor pexecutor, int ppoolSize, int pshutdownDelay) {
        transportFactory = ptransportFactory;
        executor = pexecutor;
        poolSize = ppoolSize;
        shutdownDelay = pshutdownDelay;
    }

    /**
     * Creates an AsyncRedisFactory Object with a default pool size of 10 and Shutdown delay of 10 seconds.
     * @param ptransportFactory Connection Factory to use to create {@link AsyncSocketTransport}
     * @param pexecutor Common Executor to use for aredis services
     */
    public AsyncRedisFactory(AsyncSocketTransportFactory ptransportFactory, Executor pexecutor) {
        this(ptransportFactory, pexecutor, 10, 10);
    }

    /**
     * Creates an AsyncRedisFactory Object with the default Transport Factory, a default pool size of 10 and Shutdown delay of 10 seconds.
     * @param pexecutor Common Executor to use for aredis services
     */
    public AsyncRedisFactory(Executor pexecutor) {
        this(AsyncSocketTransportFactory.getDefault(), pexecutor);
    }

    // Used for DCL. Assumed that if create is true lock has been acquired.
    private AsyncRedisService getService(String connectionString, Executor pexecutor, boolean create, boolean unalias, ServiceType serviceType) {
        Object serviceObject = null;
        AsyncRedisService asyncRedisService = null;
        Map<String, AsyncRedisService> sm = serviceMap;
        if(sm != null) {
            asyncRedisService = sm.get(connectionString);
            if(asyncRedisService != null) {
                if(serviceType == ServiceType.CONNECTION) {
                    serviceObject = asyncRedisService.connection;
                }
                else if(serviceType == ServiceType.CONNECTIONPOOL) {
                    serviceObject = asyncRedisService.connectionPool;
                }
                else {
                    serviceObject = asyncRedisService.subscription;
                }
            }
        }
        if(serviceObject == null) {
            if(create) {
                String newConnectionString = null;
                RedisServerInfo serverInfo = new RedisServerInfo(connectionString);
                if(unalias) {
                    newConnectionString = serverInfo.getRedisConnectionString();
                    if(newConnectionString.equals(connectionString)) {
                        unalias = false;
                    }
                }
                if(unalias) {
                    asyncRedisService = getService(newConnectionString, pexecutor, create, false, serviceType);
                }
                else {
                    if(asyncRedisService != null) {
                        asyncRedisService = asyncRedisService.clone();
                    }
                    else {
                        asyncRedisService = new AsyncRedisService();
                    }
                    Executor e = pexecutor;
                    if(e == null) {
                        e = executor;
                    }
                    if(serviceType == ServiceType.CONNECTION) {
                        AsyncSocketTransport con = transportFactory.getTransport(serverInfo.getHost(), serverInfo.getPort());
                        AsyncRedisConnection connection = new AsyncRedisConnection(con, serverInfo.getDbIndex(), e);
                        if(dataHandler != null) {
                            connection.setDataHandler(dataHandler);
                        }
                        asyncRedisService.connection = connection;
                        // The below 2 lines are for trying to initialize the common aredis connection with the
                        // passed transportFactory since it may not be the default
                        RedisServerWideData redisServerWideData = RedisServerWideData.getInstance(serverInfo);
                        redisServerWideData.getCommonAredisConnection(transportFactory, 0);
                    }
                    else if(serviceType == ServiceType.CONNECTIONPOOL) {
                        AsyncSocketTransportFactory tf = poolTransportFactory;
                        if(tf == null) {
                            tf = transportFactory;
                        }
                        AsyncRedisConnectionPoolManager poolManager = new AsyncRedisConnectionPoolManager(tf, serverInfo, e, shutdownDelay);
                        AsyncObjectPool<AsyncRedisConnection> connectionPool = new LeakCheckingAsyncObjectPool<AsyncRedisConnection>(poolManager, poolSize, e);
                        asyncRedisService.connectionPool = connectionPool;
                    }
                    else {
                        AsyncSocketTransportFactory tf = subscriptionTransportFactory;
                        if(tf == null) {
                            tf = transportFactory;
                        }
                        AsyncSocketTransport con = tf.getTransport(serverInfo.getHost(), serverInfo.getPort());
                        RedisSubscription subscription = new RedisSubscription(con, serverInfo.getDbIndex(), e);
                        asyncRedisService.subscription = subscription;
                    }
                }
                // Need to reassign since there is a getService Call in between which changes serviceMap
                sm = serviceMap;
                if(sm != null) {
                    sm = new HashMap<String, AsyncRedisService>(sm);
                }
                else {
                    sm = new HashMap<String, AsyncRedisService>();
                }
                sm.put(connectionString, asyncRedisService);
                serviceMap = sm;
            }
            else {
                asyncRedisService = null;
            }
        }

        return asyncRedisService;
    }

    private ShardedAsyncRedisClient getShardedAsyncRedisClient(String connectionString, Executor pexecutor, boolean create, boolean unalias) {
        ShardedAsyncRedisClient shardedClient = null;
        Map<String, ShardedAsyncRedisClient> sm = shardedConnectionMap;
        if(sm != null) {
            shardedClient = sm.get(connectionString);
        }
        int i, len;
        if(shardedClient == null && create) {
            if(connectionString.indexOf(',') <= 0) {
                throw new IllegalArgumentException("Cannot Create a Sharded Async Redis Client with only one server in Connection String " + connectionString + ". Need atleast 2 servers in Connection String");
            }
            String newConnectionString = null;
            RedisServerListInfo serverListInfo = new RedisServerListInfo(connectionString);
            if(unalias) {
                newConnectionString = serverListInfo.getConnectionString();
                if(newConnectionString.equals(connectionString)) {
                    unalias = false;
                }
            }
            if(unalias) {
                shardedClient = getShardedAsyncRedisClient(newConnectionString, pexecutor, create, false);
            }
            else {
                List<RedisServerInfo> serverList = serverListInfo.getServerList();
                len = serverList.size();
                List<AsyncRedisConnection> connectionList = new ArrayList<AsyncRedisConnection>(len);
                for(i = 0; i < len; i++) {
                    AsyncRedisConnection connection = getConnection(serverList.get(i), pexecutor);
                    connectionList.add(connection);
                }
                Executor e = pexecutor;
                if(e == null) {
                    e = executor;
                }
                shardedClient = new ShardedAsyncRedisClient(connectionList, e);
            }
            // Need to reassign since there is a getShardedAsyncRedisClient Call in between which changes shardedConnectionMap
            sm = shardedConnectionMap;
            if(sm != null) {
                sm = new HashMap<String, ShardedAsyncRedisClient>(sm);
            }
            else {
                sm = new HashMap<String, ShardedAsyncRedisClient>();
            }
            sm.put(connectionString, shardedClient);
            shardedConnectionMap = sm;
        }

        return shardedClient;
    }

    /**
     * Returns an AsycRedisConnection. However the preferred method is getClient.
     * @param redisServerInfo Redis Server Info containing host, port and DB Index
     * @param pexecutor Specific Executor to use for the connection returned or null to use the Factory's Executor.
     * @return An AsyncRedisConnection for the given server
     */
    public AsyncRedisConnection getConnection(RedisServerInfo redisServerInfo, Executor pexecutor) {
        String connectionString = redisServerInfo.getRedisConnectionString();
        AsyncRedisService asyncRedisService = getService(connectionString , pexecutor, false, false, ServiceType.CONNECTION);
        if(asyncRedisService == null) {
            synchronized(this) {
                asyncRedisService = getService(connectionString, pexecutor, true, false, ServiceType.CONNECTION);
            }
        }

        return asyncRedisService.connection;
    }

    /**
     * Returns an AsycRedisConnection. However the preferred method is getClient.
     * @param redisServerInfo Redis Server Info containing host, port and DB Index
     * @return An AsyncRedisConnection for the given server
     */
    public AsyncRedisConnection getConnection(RedisServerInfo redisServerInfo) {
        return getConnection(redisServerInfo, null);
    }

    /**
     * Returns a connection pool for use in Redis Transactions with WATCH-MULTI-EXEC commands.
     * @param redisServerInfo Redis Server Info containing host, port and DB Index
     * @param pexecutor Specific Executor to use for the connection pool returned or null to use the Factory's Executor.
     * @return An AsyncObjectPool holding AsyncRedisConnection objects
     */
    public AsyncObjectPool<AsyncRedisConnection> getConnectionPool(RedisServerInfo redisServerInfo, Executor pexecutor) {
        String connectionString = redisServerInfo.getRedisConnectionString();
        AsyncRedisService asyncRedisService = getService(connectionString , pexecutor, false, false, ServiceType.CONNECTIONPOOL);
        if(asyncRedisService == null) {
            synchronized(this) {
                asyncRedisService = getService(connectionString, pexecutor, true, false, ServiceType.CONNECTIONPOOL);
            }
        }

        return asyncRedisService.connectionPool;
    }

    /**
     * Returns a connection pool for use in Redis Transactions with WATCH-MULTI-EXEC commands.
     * @param redisServerInfo Redis Server Info containing host, port and DB Index
     * @return An AsyncObjectPool holding AsyncRedisConnection objects
     */
    public AsyncObjectPool<AsyncRedisConnection> getConnectionPool(RedisServerInfo redisServerInfo) {
        return getConnectionPool(redisServerInfo, null);
    }

    /**
     * Returns an AsyncRedisClient from a connection string containing one or more redis servers.
     * The connection string is a comma separated list of Servers with each Server containing the host, port followed
     * by a : and redis db index followed by a /. The port (Defaults to 6379) and db index (Defaults to 0) are optional.
     * Example connection Strings: "localhost", "10.12.33.44:6380", "server1/0,server2:6380,server3:6379/2".
     * The returned client is an instance of AsyncRedisConnection with ConnectionType SHARED if the connection string
     * contains only server else it is an instance of ShardedAsyncRedisConnection.
     * @param connectionString Connection String
     * @param pexecutor Specific Executor to use for the client returned or null to use the Factory's Executor.
     * @return An AsyncRedisClient to submit redis commands
     */
    public AsyncRedisClient getClient(String connectionString, Executor pexecutor) {
        AsyncRedisClient asyncRedisClient = null;
        if(connectionString.indexOf(',') <= 0) {
            AsyncRedisService asyncRedisService = getService(connectionString , pexecutor, false, false, ServiceType.CONNECTION);
            if(asyncRedisService == null) {
                synchronized(this) {
                    asyncRedisService = getService(connectionString, pexecutor, true, true, ServiceType.CONNECTION);
                }
            }
            asyncRedisClient = asyncRedisService.connection;
        }
        else {
            asyncRedisClient = getShardedAsyncRedisClient(connectionString, pexecutor, false, false);
            if(asyncRedisClient == null) {
                synchronized(this) {
                    asyncRedisClient = getShardedAsyncRedisClient(connectionString, pexecutor, true, true);
                }
            }
        }

        return asyncRedisClient;
    }

    /**
     * Returns an AsyncRedisClient from a connection string containing one or more redis servers.
     * The connection string is a comma separated list of Servers with each Server containing the host, port followed
     * by a : and redis db index followed by a /. The port (Defaults to 6379) and db index (Defaults to 0) are optional.
     * Example connection Strings: "localhost", "10.12.33.44:6380", "server1/0,server2:6380,server3:6379/2".
     * The returned client is an instance of AsyncRedisConnection with ConnectionType SHARED if the connection string
     * contains only server else it is an instance of ShardedAsyncRedisConnection.
     * @param connectionString Connection String
     * @return An AsyncRedisClient to submit redis commands
     */
    public AsyncRedisClient getClient(String connectionString) {
        return getClient(connectionString, null);
    }

    /**
     * Returns an AsyncObjectPool holding AsyncRedisConnection objects for WATCH-MULTI-EXEC redis transactions.
     * @param connectionString Connection String specifying a single redis server
     * @param pexecutor Specific Executor to use for the pool returned or null to use the Factory's Executor.
     * @return An Object Pool of AsyncRedisConnection from which one can borrow a connection, run WATCH-MULTI-EXEC along with other commands and
     * return the connection to the pool on completion of EXEC
     */
    public AsyncObjectPool<AsyncRedisConnection> getConnectionPool(String connectionString, Executor pexecutor) {
        if(connectionString.indexOf(',') >= 0) {
            throw new IllegalArgumentException("Invalid character \",\" (comma) in Connection String: " + connectionString + " only one server allowed for getConnectionPool");
        }
        AsyncRedisService asyncRedisService = getService(connectionString , pexecutor, false, false, ServiceType.CONNECTIONPOOL);
        if(asyncRedisService == null) {
            synchronized(this) {
                asyncRedisService = getService(connectionString, pexecutor, true, true, ServiceType.CONNECTIONPOOL);
            }
        }

        return asyncRedisService.connectionPool;
    }

    /**
     * Returns an AsyncObjectPool holding AsyncRedisConnection objects for WATCH-MULTI-EXEC redis transactions.
     * @param connectionString Connection String specifying a single redis server
     * @return An Object Pool of AsyncRedisConnection
     */
    public AsyncObjectPool<AsyncRedisConnection> getConnectionPool(String connectionString) {
        return getConnectionPool(connectionString, null);
    }

    /**
     * Returns a RedisSubscription object for subscribing to messages and message patterns on the given Redis Server.
     * @param redisServerInfo Redis Server Info containing host, port and DB Index
     * @param pexecutor Specific Executor to use for the RedisSubscription returned or null to use the Factory's Executor.
     * @return A RedisSubscription object using which one can Subscribe to messages on the Redis Server
     */
    public RedisSubscription getSubscription(RedisServerInfo redisServerInfo, Executor pexecutor) {
        String connectionString = redisServerInfo.getRedisConnectionString();
        AsyncRedisService asyncRedisService = getService(connectionString , pexecutor, false, false, ServiceType.SUBSCRIPTION);
        if(asyncRedisService == null) {
            synchronized(this) {
                asyncRedisService = getService(connectionString, pexecutor, true, false, ServiceType.SUBSCRIPTION);
            }
        }

        return asyncRedisService.subscription;
    }

    /**
     * Returns a RedisSubscription object for subscribing to messages and message patterns on the given Redis Server.
     * @param redisServerInfo Redis Server Info containing host, port and DB Index
     * @return A RedisSubscription object using which one can Subscribe to messages on the Redis Server
     */
    public RedisSubscription getSubscription(RedisServerInfo redisServerInfo) {
        return getSubscription(redisServerInfo, null);
    }

    /**
     * Returns a RedisSubscription object for subscribing to messages and message patterns on the given Redis Server.
     * @param connectionString Connection String specifying a single redis server
     * @param pexecutor Specific Executor to use for the RedisSubscription returned or null to use the Factory's Executor.
     * @return A RedisSubscription object using which one can Subscribe to messages on the Redis Server
     */
    public RedisSubscription getSubscription(String connectionString, Executor pexecutor) {
        if(connectionString.indexOf(',') >= 0) {
            throw new IllegalArgumentException("Invalid character \",\" (comma) in Connection String: " + connectionString + " only one server allowed for getConnectionPool");
        }
        AsyncRedisService asyncRedisService = getService(connectionString , pexecutor, false, false, ServiceType.SUBSCRIPTION);
        if(asyncRedisService == null) {
            synchronized(this) {
                asyncRedisService = getService(connectionString, pexecutor, true, true, ServiceType.SUBSCRIPTION);
            }
        }

        return asyncRedisService.subscription;
    }

    /**
     * Returns a RedisSubscription object for subscribing to messages and message patterns on the given Redis Server.
     * @param connectionString Connection String specifying a single redis server
     * @return A RedisSubscription object using which one can Subscribe to messages on the Redis Server
     */
    public RedisSubscription getSubscription(String connectionString) {
        return getSubscription(connectionString, null);
    }

    /**
     * Gets the static instance of RedisTimer in use for periodic tasks re-connects, Leak Checks etc. If none is set a new DefaultRedisTimer is created an returned.
     * @return The RedisTimer in use
     */
    public static RedisTimer getTimer() {
        if(timer == null) {
            synchronized(AsyncRedisFactory.class) {
                if(timer == null) {
                    timer = new DefaultRedisTimer();
                }
            }
        }

        return timer;
    }

    /**
     * Sets the RedisTimer to use. This is for overriding the default Timer if you already have a Timer.
     * @param ptimer Timer to set
     */
    public static void setTimer(RedisTimer ptimer) {
        synchronized(AsyncRedisFactory.class) {
            if(timer == null) {
                timer = ptimer;
            }
            else {
                log.warn("Ignoring setTimer Call since timer has already been initialized/set");
            }
        }
    }

    /**
     * Gets the Data Handler used by the factory.
     * @return The Default Data Handler used when creating a redis service or null if none set
     */
    public DataHandler getDataHandler() {
        return dataHandler;
    }

    /**
     * Sets the Data Handler used by the factory
     * @param pdataHandler The Default Data Handler to use when creating a redis service or null if none set
     */
    public void setDataHandler(DataHandler pdataHandler) {
        dataHandler = pdataHandler;
    }

    /**
     * Gets the pool size used when creating a connection pool
     * @return pool size
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Sets the pool size to use when creating a connection pool
     * @param ppoolSize pool size
     */
    public void setPoolSize(int ppoolSize) {
        poolSize = ppoolSize;
    }

    /**
     * Gets the Transport Factory used when creating a connection pool.
     * @return Transport factory for connection pools or null if none is set in which case the Factory's Transport Factory is used
     */
    public AsyncSocketTransportFactory pgetPoolTransportFactory() {
        return poolTransportFactory;
    }

    /**
     * Sets the Transport Factory to use when creating a connection pool. This could be used to configure the transport factory
     * with a different idleTimeout to be used only for connection pools for example.
     * @param ppoolTransportFactory Transport factory for connection pools
     */
    public void setPoolTransportFactory(
            AsyncSocketTransportFactory ppoolTransportFactory) {
        poolTransportFactory = ppoolTransportFactory;
    }

    /**
     * Gets the Transport Factory used for RedisSuubscription service or null if none is set.
     * @return Transport Factory for RedisSubscription
     */
    public AsyncSocketTransportFactory getSubscriptionTransportFactory() {
        return subscriptionTransportFactory;
    }

    /**
     * Sets the Transport Factory to Use for RedisSuubscription service.
     * @param psubscriptionTransportFactory Transport Factory for RedisSubscription
     */
    public void setSubscriptionTransportFactory(
            AsyncSocketTransportFactory psubscriptionTransportFactory) {
        subscriptionTransportFactory = psubscriptionTransportFactory;
    }

    /**
     * Gets the default Transport Factory which is also used for AsyncRedisConnections.
     * @return Default Transport Factory used by this AsyncRedisFactory
     */
    public AsyncSocketTransportFactory getTransportFactory() {
        return transportFactory;
    }

    /**
     * Gets the configured Executor for this factory or null if none is configured.
     * @return
     */
    public Executor getExecutor() {
        return executor;
    }
}
