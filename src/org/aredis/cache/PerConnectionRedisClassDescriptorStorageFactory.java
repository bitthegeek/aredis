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

import java.util.HashMap;
import java.util.Map;

import org.aredis.net.AsyncSocketTransport;
import org.aredis.net.AsyncSocketTransportFactory;
import org.aredis.net.ServerInfo;

/**
 * A ClassDescriptorStorage Factory which returns a {@link RedisClassDescriptorStorage} for the given redis server.
 * This is the default used by aredis.
 *
 * @author Suresh
 *
 */
public class PerConnectionRedisClassDescriptorStorageFactory implements ClassDescriptorStorageFactory {

    private int dbIndex;

    private AsyncSocketTransportFactory asyncSocketTransportFactory;

    private String descriptorsKey;

    private volatile Map<String, RedisClassDescriptorStorage> serverStorageMap;

    /**
     * Creates a factory object.
     *
     * @param pasyncSocketTransportFactory factory to create AsyncSocketTransport for AsyncRedisConnection
     * @param pdbIndex dbIndex to use
     */
    public PerConnectionRedisClassDescriptorStorageFactory(AsyncSocketTransportFactory pasyncSocketTransportFactory, int pdbIndex) {
        asyncSocketTransportFactory = pasyncSocketTransportFactory;
        dbIndex = pdbIndex;
        descriptorsKey = RedisClassDescriptorStorage.DEFAULT_DESCRIPTORS_KEY;
    }

    /**
     * Creates a factory object. dbIndex used is 0.
     *
     * @param pasyncSocketTransportFactory factory to create AsyncSocketTransport for AsyncRedisConnection
     */
    public PerConnectionRedisClassDescriptorStorageFactory(AsyncSocketTransportFactory pasyncSocketTransportFactory) {
        this(pasyncSocketTransportFactory, 0);
    }

    /**
     * Gets the storage to use for the passed server. The same Object is returned for subsequent calls for the same
     * server.
     */
    @Override
    public RedisClassDescriptorStorage getStorage(ServerInfo conInfo) {
        RedisClassDescriptorStorage storage = null;
        Map<String, RedisClassDescriptorStorage> map = serverStorageMap;
        String serverKey = conInfo.getConnectionString();
        if(map != null) {
            storage = map.get(serverKey);
        }
        if(storage == null) {
            synchronized(this) {
                // DCL
                if(map != serverStorageMap) {
                    map = serverStorageMap;
                    if(map != null) {
                        storage = map.get(serverKey);
                    }
                }
                if(storage == null) {
                    AsyncSocketTransportFactory tf = asyncSocketTransportFactory;
                    if(tf == null) {
                        tf = AsyncSocketTransportFactory.getDefault();
                    }
                    AsyncSocketTransport con = tf.getTransport(conInfo.getHost(), conInfo.getPort());
                    AsyncRedisConnection aredis = new AsyncRedisConnection(con, dbIndex, null, ConnectionType.STANDALONE);
                    storage = new RedisClassDescriptorStorage(aredis, descriptorsKey);
                    if(map != null) {
                        map = new HashMap<String, RedisClassDescriptorStorage>(map);
                    }
                    else {
                        map = new HashMap<String, RedisClassDescriptorStorage>();
                    }
                    map.put(serverKey, storage);
                    serverStorageMap = map;
                }
            }
        }
        return storage;
    }

    /**
     * Gets the key in which the Descriptors are stored. The default is JAVA_CL_DESCRIPTORS.
     *
     * @return Descriptors key
     */
    public String getDescriptorsKey() {
        return descriptorsKey;
    }

    /**
     * Sets the descriptors key to use.
     *
     * @param pdescriptorsKey Key to use for storing the descriptors.
     */
    public void setDescriptorsKey(String pdescriptorsKey) {
        descriptorsKey = pdescriptorsKey;
    }

}
