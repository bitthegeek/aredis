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

import java.util.concurrent.Future;

/**
 * A basic implementation of AsyncRedisClient based on 2 abstract methods. The rest of the calls pass appropriate defaults to one of the abstract methods.
 * @author suresh
 *
 */
public abstract class AbstractAsyncRedisClient implements AsyncRedisClient {
    @Override
    public abstract Future<RedisCommandInfo[]> submitCommands(RedisCommandInfo commands[], AsyncHandler<RedisCommandInfo[]> completionHandler, boolean requireFutureResult, boolean isSyncCallback);

    @Override
    public abstract Future<RedisCommandInfo> submitCommand(RedisCommandInfo command, AsyncHandler<RedisCommandInfo> completionHandler, boolean requireFutureResult, boolean isSyncCallback);

    /**
     * @see org.aredis.cache.AsyncRedisClient#submitCommands(org.aredis.cache.RedisCommandInfo[])
     */
    @Override
    public Future<RedisCommandInfo[]> submitCommands(RedisCommandInfo commands[]) {
        return submitCommands(commands, null, true, false);
    }

    /**
     * @see org.aredis.cache.AsyncRedisClient#submitCommands(org.aredis.cache.RedisCommandInfo[], org.aredis.cache.AsyncHandler)
     */
    @Override
    public void submitCommands(RedisCommandInfo commands[], AsyncHandler<RedisCommandInfo[]> completionHandler) {
        submitCommands(commands, completionHandler, false, false);
    }

    /**
     * @see org.aredis.cache.AsyncRedisClient#submitCommand(org.aredis.cache.RedisCommandInfo)
     */
    @Override
    public Future<RedisCommandInfo> submitCommand(RedisCommandInfo command) {
        return submitCommand(command, null, true, false);
    }

    /**
     * @see org.aredis.cache.AsyncRedisClient#submitCommand(org.aredis.cache.RedisCommandInfo, org.aredis.cache.AsyncHandler)
     */
    @Override
    public void submitCommand(RedisCommandInfo command, AsyncHandler<RedisCommandInfo> completionHandler) {
        submitCommand(command, completionHandler, false, false);
    }

    @Override
    public Future<RedisCommandInfo> submitCommand(RedisCommand command,
            Object... params) {
        return submitCommand(new RedisCommandInfo(null, null, command, params), null, true, false);
    }

    @Override
    public Future<RedisCommandInfo> submitCommand(Object metaData,
            RedisCommand command, Object... params) {
        return submitCommand(new RedisCommandInfo(null, metaData, command, params), null, true, false);
    }

    @Override
    public Future<RedisCommandInfo> submitCommand(DataHandler dataHandler,
            RedisCommand command, Object... params) {
        return submitCommand(new RedisCommandInfo(dataHandler, null, command, params), null, true, false);
    }

    @Override
    public Future<RedisCommandInfo> submitCommand(DataHandler dataHandler,
            Object metaData, RedisCommand command, Object... params) {
        return submitCommand(new RedisCommandInfo(dataHandler, metaData, command, params), null, true, false);
    }

    @Override
    public void sendCommand(RedisCommand command, Object... params) {
        submitCommand(new RedisCommandInfo(null, null, command, params), null, false, false);
    }

    @Override
    public void sendCommand(Object metaData, RedisCommand command, Object... params) {
        submitCommand(new RedisCommandInfo(null, metaData, command, params), null, false, false);
    }

    @Override
    public void sendCommand(DataHandler dataHandler, RedisCommand command, Object... params) {
        submitCommand(new RedisCommandInfo(dataHandler, null, command, params), null, false, false);
    }

    @Override
    public void sendCommand(DataHandler dataHandler, Object metaData, RedisCommand command,
            Object... params) {
        submitCommand(new RedisCommandInfo(dataHandler, metaData, command, params), null, false, false);
    }

    @Override
    public void submitCommand(AsyncHandler<RedisCommandInfo> completionHandler,
            RedisCommand command, Object... params) {
        submitCommand(new RedisCommandInfo(null, null, command, params), completionHandler, false, false);
    }

    @Override
    public void submitCommand(AsyncHandler<RedisCommandInfo> completionHandler,
            Object metaData, RedisCommand command, Object... params) {
        submitCommand(new RedisCommandInfo(null, metaData, command, params), completionHandler, false, false);
    }

    @Override
    public void submitCommand(AsyncHandler<RedisCommandInfo> completionHandler,
            DataHandler dataHandler, RedisCommand command, Object... params) {
        submitCommand(new RedisCommandInfo(dataHandler, null, command, params), completionHandler, false, false);
    }

    @Override
    public void submitCommand(AsyncHandler<RedisCommandInfo> completionHandler,
            DataHandler dataHandler, Object metaData, RedisCommand command,
            Object... params) {
        submitCommand(new RedisCommandInfo(dataHandler, metaData, command, params), completionHandler, false, false);
    }

}
