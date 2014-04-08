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

import org.aredis.net.AsyncSocketTransportFactory;
import org.aredis.net.ServerInfo;

/**
 * A ClassDescriptorStorage Factory which a returns a single {@link RedisClassDescriptorStorage} for all redis servers.
 * Configure this if you want the Class descriptors for all redis servers to reside on a common redis server.
 *
 * @author Suresh
 *
 */
public class RedisClassDescriptorStorageFactory implements ClassDescriptorStorageFactory {

    private RedisClassDescriptorStorage redisClassDescriptorStorage;

    /**
     * Creates a factory along with the {@link RedisClassDescriptorStorage} to return.
     *
     * @param redisServerInfo Redis Server Info where descriptors are to be stored
     * @param pdescriptorsKey Descriptors key to use
     * @param asyncSocketTransportFactory factory to create AsyncSocketTransport for AsyncRedisConnection
     */
    public RedisClassDescriptorStorageFactory(RedisServerInfo redisServerInfo, String pdescriptorsKey, AsyncSocketTransportFactory asyncSocketTransportFactory) {
        RedisServerWideData redisServerWideData = RedisServerWideData.getInstance(redisServerInfo);
        AsyncRedisConnection aredis = redisServerWideData.getCommonAredisConnection(asyncSocketTransportFactory, redisServerInfo.getDbIndex());
        redisClassDescriptorStorage = new RedisClassDescriptorStorage(aredis, pdescriptorsKey, redisServerInfo.getDbIndex());
    }

    /**
     * Creates a factory along with the {@link RedisClassDescriptorStorage} to return. The dbIndex defaults to 0 and
     * the default key JAVA_CL_DESCRIPTORS is used to store the descriptors.
     *
     * @param redisServerInfo Redis Server Info where descriptors are to be stored
     */
    public RedisClassDescriptorStorageFactory(RedisServerInfo redisServerInfo) {
        this(redisServerInfo, null, null);
    }

    /**
     * Returns the storage. The passed server is ignored and the same storage Object is returned for all calls.
     */
    @Override
    public RedisClassDescriptorStorage getStorage(ServerInfo conInfo) {
        return redisClassDescriptorStorage;
    }
}
