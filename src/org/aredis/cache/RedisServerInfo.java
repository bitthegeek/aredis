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

import org.aredis.net.ServerInfo;
import org.aredis.net.ServerInfoComparator;
import org.aredis.util.SortedArray.IndexUpdater;

/**
 * Identifies a Redis server with its host, port and dbIndex. Provides an implementation of the hashCode, equals and compareTo methods
 * also so that this Object can be used as a key.
 * @author Suresh
 *
 */
public class RedisServerInfo implements ServerInfo, Comparable<RedisServerInfo> {

    /** Cache the hash code. Not using volatile as is done in String. With int non-volatile will either get
     * 0 or the latest. With long volatile would be required since otherwise only leading/trailing 32 bits may show */
    private int hash; // Default to 0

    private String host;

    private int port;

    private int dbIndex;

    private String connectionString;

    private int serverIndex;

    private String redisConnectionString;

    private void init(String phost, int pport, int pdbIndex) {
        host = phost;
        port = pport;
        connectionString = host + ':' + port;
        dbIndex = pdbIndex;
        redisConnectionString = host + ':' + port + '/' + dbIndex;
        ServerInfo t = ServerInfoComparator.findItem(this, new IndexUpdater() {
            @Override
            public void updateIndex(int index) {
                serverIndex = index;
            }
        });
        if (t != this) {
            serverIndex = t.getServerIndex();
        }
    }

    /**
     * Creates a RedisServerInfo Object.
     * @param phost Server host
     * @param pport Server port
     * @param pdbIndex DB Index to use
     */
    public RedisServerInfo(String phost, int pport, int pdbIndex) {
        init(phost, pport, pdbIndex);
    }

    /**
     * Creates a RedisServerInfo Object with 0 as the DB Index.
     * @param phost Server host
     * @param pport Server port
     */
    public RedisServerInfo(String phost, int pport) {
        this(phost, pport, 0);
    }

    /**
     * Creates a RedisServerInfo Object from a Connection String. The Connection String contains the host, port followed
     * by a : and redis db index followed by a /. The port defaults to 6379 if omitted and the DB Index defaults to 0.
     * Example Connection Strings are "10.10.43.3", "localhost:6379", "10.10.23.4/2", "localhost:6380/3".
     * @param connectionString Connection String
     */
    public RedisServerInfo(String connectionString) {
        try {
            int dbIndex = 0;
            int len = connectionString.length();
            int endPos = len;
            int pos = connectionString.indexOf('/');
            if(pos > 0) {
                endPos = pos;
                dbIndex = Integer.parseInt(connectionString.substring(pos + 1).trim());
            }
            int port = 6379;
            pos = connectionString.indexOf(':');
            if(pos > 0) {
                port = Integer.parseInt(connectionString.substring(pos + 1, endPos).trim());
                endPos = pos;
            }
            String host = connectionString;
            if(endPos != len) {
                host = connectionString.substring(0, endPos).trim();
            }
            init(host, port, dbIndex);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Invalid Connection String: " + connectionString, e);
        }
    }

    @Override
    public int hashCode() {
        if(hash == 0) {
            hash = redisConnectionString.hashCode();
        }

        return hash;
    }

    @Override
    public boolean equals(Object o) {
        boolean result = false;
        if(this == o) {
            result = true;
        }
        else if(o instanceof RedisServerInfo) {
            RedisServerInfo redisServerInfo = (RedisServerInfo) o;
            String otherHost = redisServerInfo.host;
            if(host == otherHost || host != null && host.equals(otherHost)) {
                if(port == redisServerInfo.port && dbIndex == redisServerInfo.dbIndex) {
                    result = true;
                }
            }
        }

        return result;
    }

    @Override
    public int compareTo(RedisServerInfo o) {
        return redisConnectionString.compareTo(o.redisConnectionString);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getDbIndex() {
        return dbIndex;
    }

    @Override
    public String getConnectionString() {
        return connectionString;
    }

    @Override
    public int getServerIndex() {
        return serverIndex;
    }

    public String getRedisConnectionString() {
        return redisConnectionString;
    }
}
