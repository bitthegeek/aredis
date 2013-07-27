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

import java.util.concurrent.Executor;

import org.aredis.net.AsyncSocketTransport;
import org.aredis.net.AsyncSocketTransportFactory;
import org.aredis.util.pool.AsyncObjectPool;
import org.aredis.util.pool.AsyncPoolMemberManager;

/**
 * A Pool Member Manager for AsyncRedisConnection so that {@link AsyncObjectPool} can be used to create a pool of
 * AsyncRedisConnection. This is used internally by {@link AsyncRedisFactory} to create a connection pool when
 * requested for a redis server.
 * @author Suresh
 *
 */
public class AsyncRedisConnectionPoolManager implements AsyncPoolMemberManager<AsyncRedisConnection>{

    private AsyncSocketTransportFactory connectionFactory;

    private RedisServerInfo redisServerInfo;

    private Executor executor;

    int shutdownDelay;

    /**
     * Creates a Pool Member manager with necessary information to create {@link AsyncRedisConnection} objects.
     * @param pconnectionFactory Connection Factory to use to create {@link AsyncSocketTransport}
     * @param predisServerInfo Server Info containing redis server and port
     * @param pexecutor Executor to use in case Async Borrow is required
     * @param pshutdownDelay Shut Down delay to use when freeing Objects
     */
    public AsyncRedisConnectionPoolManager(AsyncSocketTransportFactory pconnectionFactory, RedisServerInfo predisServerInfo, Executor pexecutor, int pshutdownDelay) {
        connectionFactory = pconnectionFactory;
        if(pconnectionFactory == null) {
            connectionFactory = AsyncSocketTransportFactory.getDefault();
        }
        redisServerInfo = predisServerInfo;
        executor = pexecutor;
        shutdownDelay = pshutdownDelay;
    }

    public AsyncRedisConnectionPoolManager(RedisServerInfo pserverInfo, Executor pexecutor) {
        this(null, pserverInfo, null, 10);
    }

    /**
     * Creates an AsyncRedisConnection.
     */
    @Override
    public AsyncRedisConnection createObject() {
        AsyncSocketTransport socketTransport = connectionFactory.getTransport(redisServerInfo.getHost(), redisServerInfo.getPort());
        AsyncRedisConnection aredisConnection = new AsyncRedisConnection(socketTransport, redisServerInfo.getDbIndex(), executor, ConnectionType.BORROWED);
        return aredisConnection;
    }

    @Override
    public void destroyObject(AsyncRedisConnection con) {
        con.shutdown(shutdownDelay);
    }

    @Override
    public void onBorrow(AsyncRedisConnection con) {
        con.isBorrowed = true;
        con.hasSelectCommands = false;
    }

    @Override
    public void beforeReturn(AsyncRedisConnection con) {
        if(con.hasSelectCommands) {
            con.submitCommand(new RedisCommandInfo(RedisCommand.SELECT, String.valueOf(con.getDbIndex())), null, false);
        }
        con.isBorrowed = false;
    }

}
