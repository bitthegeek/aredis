/*
 * Copyright (C) 2014 Suresh Mahalingam.  All rights reserved.
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
