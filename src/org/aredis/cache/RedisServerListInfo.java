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

import java.util.Arrays;
import java.util.List;

import org.aredis.util.ArrayWrappingList;

/**
 * Identifies a list of Redis servers. Provides an implementation of the hashCode, equals and compareTo methods
 * also so that this Object can be used as a key.
 * @author Suresh
 *
 */
public class RedisServerListInfo implements Comparable<RedisServerListInfo> {

    private int hash;

    private RedisServerInfo servers[];

    private List<RedisServerInfo> serverList;

    private String connectionString;

    private void init(RedisServerInfo[] pservers) {
        if(pservers.length == 0) {
            throw new IllegalArgumentException("Empty Server List");
        }
        servers = Arrays.copyOf(pservers, pservers.length);
        Arrays.sort(servers);
        StringBuilder sb = new StringBuilder();
        int i;
        for(i = 0; i < servers.length; i++) {
            sb.append(servers[i].getRedisConnectionString()).append(',');
        }
        sb.setLength(sb.length() - 1);
        connectionString = sb.toString();
        serverList = new ArrayWrappingList<RedisServerInfo>(servers);
    }

    /**
     * Creates a RedisServerListInfo from Individual RedisServerInfo's
     * @param pservers Individual Redis Server Infos
     */
    public RedisServerListInfo(RedisServerInfo pservers[]) {
        init(pservers);
    }

    /**
     * Creates a RedisServerListInfo from a Connection String containing a comma separated list of RedisServerInfos.
     * Examples are: "localhost/2,10.10.23.3:6381", "10.10.10.33".
     * @param connectionString Connection String
     */
    public RedisServerListInfo(String connectionString) {
        int i;
        String connectionStrings[] = connectionString.split(",");
        RedisServerInfo servers[] = new RedisServerInfo[connectionStrings.length];
        for(i = 0; i < connectionStrings.length; i++) {
            servers[i] = new RedisServerInfo(connectionStrings[i]);
        }
        init(servers);
    }

    @Override
    public int hashCode() {
        if(hash == 0) {
            hash = connectionString.hashCode();
        }

        return hash;
    }

    @Override
    public boolean equals(Object o) {
        boolean result = false;
        if(this == o) {
            result = true;
        }
        else if(o instanceof RedisServerListInfo) {
            RedisServerListInfo serverListInfo = (RedisServerListInfo) o;
            result = Arrays.equals(servers, serverListInfo.servers);
        }

        return result;
    }

    @Override
    public int compareTo(RedisServerListInfo o) {
        return connectionString.compareTo(o.connectionString);
    }

    public List<RedisServerInfo> getServerList() {
        return serverList;
    }

    public String getConnectionString() {
        return connectionString;
    }
}
