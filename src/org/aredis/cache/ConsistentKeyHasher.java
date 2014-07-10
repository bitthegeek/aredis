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
import java.util.Collections;
import java.util.List;

import org.aredis.net.AsyncSocketTransport;
import org.aredis.net.ConnectionStatus;

/**
 * Consistent Hashing Algorithm derived from the one in SpyMemcached memcached client. Essentially a number of hashcodes
 * are generated for each server by adding numeric suffixes to each server's connection string and generating the hashcode
 * when this Key Hasher is created. When a key is to be assigned the nearest hashcode greater
 * than the hashcode of the key is determined whose connection is active.
 * (The server hashcodes are treated as points on a circle. So if the there is no greater hashcode the smallest server hashcode is used)
 * The corresponding Connection Index is used.
 * This algorithm re-destributes keys uniformly when a new server is added or removed to the Sharded Set of servers.
 * @author suresh
 *
 */
public class ConsistentKeyHasher implements KeyHasher {
    private AsyncRedisConnection connections[];

    private HashAlgorithm hashAlg;

    private int hashesPerConnection;

    private int sortedConnectionHashes[];

    private int connectionIndexes[];

    private static class TempHashInfo implements Comparable<TempHashInfo> {

        private int connectionHash;

        private int connectionIndex;

        @Override
        public int compareTo(TempHashInfo otherHashInfo) {
            int result;
            if(connectionHash < otherHashInfo.connectionHash) {
                result = -1;
            }
            else if(connectionHash > otherHashInfo.connectionHash) {
                result = 1;
            }
            else {
                result = connectionIndex - otherHashInfo.connectionIndex;
            }

            return result;
        }

    }

    // Variation of jdk version which does not match but only finds insertion point
    private static int binarySearch(int[] a, int key) {
        int low = 0;
        int high = a.length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = a[mid];

            if (midVal < key)
                low = mid + 1;
            else
                high = mid - 1;
        }
        return low;
    }

    private String getKeyToHash(AsyncRedisConnection connection, int repetitionIndex) {
        AsyncSocketTransport con = connection.getConnection();
        int javaHashCode = (con.getHost() + ':' + con.getPort() + '/' + connection.getDbIndex()).hashCode();
        String serverNameRandomizer1 = String.valueOf(javaHashCode % 10);
        String serverNameRandomizer2 = String.valueOf((javaHashCode / 10) % 10);
        String connectionKeyToHash = "" + serverNameRandomizer1 + serverNameRandomizer2 + '-' + repetitionIndex + '-' + con.getHost() + '-' + serverNameRandomizer2 + serverNameRandomizer1 + ':' + con.getPort() + '/' + connection.getDbIndex() + '-' + repetitionIndex + '-' + serverNameRandomizer2 + serverNameRandomizer1;
        return connectionKeyToHash;
    }

    /**
     * Creates a Consistent Key Hasher.
     * @param pconnections connections to pick one from
     * @param phashesPerConnection Number of Hashcodes to generate for each connection
     * @param phashAlg Hash Algorithm to generate the hash codes of the key and the servers
     */
    public ConsistentKeyHasher(AsyncRedisConnection[] pconnections, int phashesPerConnection, HashAlgorithm phashAlg) {
        connections = pconnections;
        hashesPerConnection = phashesPerConnection;
        hashAlg = phashAlg;
        if(connections.length > 1) {
            TempHashInfo tempHashInfo;
            List<TempHashInfo> tempHashInfos = new ArrayList<ConsistentKeyHasher.TempHashInfo>(hashesPerConnection * connections.length);
            int connectionIndex;
            for(connectionIndex = 0; connectionIndex < connections.length; connectionIndex++) {
                AsyncRedisConnection connection = connections[connectionIndex];
                if (hashAlg == DefaultHashAlgorithm.KETAMA_HASH) {
                    for (int i = 0; i < hashesPerConnection / 4; i++) {
                        byte[] digest = DefaultHashAlgorithm.computeMd5(getKeyToHash(connection, i));
                        for (int h = 0; h < 4; h++) {
                            long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                                    | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                                    | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                                    | (digest[h * 4] & 0xFF);
                            tempHashInfo = new TempHashInfo();
                            tempHashInfo.connectionHash = (int) k;
                            tempHashInfo.connectionIndex = connectionIndex;
                            tempHashInfos.add(tempHashInfo);
                        }
                    }
                } else {
                    for (int i = 0; i < hashesPerConnection; i++) {
                        tempHashInfo = new TempHashInfo();
                        tempHashInfo.connectionHash = hashAlg.hash(getKeyToHash(connection, i));
                        tempHashInfo.connectionIndex = connectionIndex;
                        tempHashInfos.add(tempHashInfo);
                    }
                }
            }
            Collections.sort(tempHashInfos);
            int len = tempHashInfos.size();
            sortedConnectionHashes = new int[len];
            connectionIndexes = new int[len];
            len = 0;
            for(TempHashInfo hi : tempHashInfos) {
                sortedConnectionHashes[len] = hi.connectionHash;
                connectionIndexes[len] = hi.connectionIndex;
                len++;
            }
        }
    }

    /**
     * Creates a Consistent Key Hasher with 180 hashes per connection and CRC_HASH as the Hash Algorithm.
     * @param pconnections connections to pick one from
     */
    public ConsistentKeyHasher(AsyncRedisConnection[] pconnections) {
        this(pconnections, 180, DefaultHashAlgorithm.CRC_HASH);
    }

    @Override
    public int getConnectionIndex(String key,
            AsyncRedisConnection[] pconnections, boolean useOnlyActive) {
        if(connections != pconnections) {
            throw new IllegalArgumentException("Connections array passed during getConnectionIndex is not the same as that passed in constructor");
        }

        int i, pos = 0;

        if(connections.length > 1) {
            int hash = hashAlg.hash(key);
            pos = binarySearch(sortedConnectionHashes, hash);
            if(pos >= sortedConnectionHashes.length) {
                pos = 0;
            }
            if(useOnlyActive) {
                ConnectionStatus status = connections[connectionIndexes[pos]].getConnection().getStatus();
                if(status == ConnectionStatus.DOWN || status == ConnectionStatus.RETRY) {
                    boolean useable[] = new boolean[connections.length];
                    int statusCode = ShardedAsyncRedisClient.getUseableConnections(connections, useable);
                    if(statusCode >= 0) {
                        pos = statusCode;
                    }
                    else if(statusCode != -connections.length) {
                        i = pos;
                        do {
                            if(useable[connectionIndexes[i]]) {
                                pos = i;
                                break;
                            }
                            i++;
                            if(i >= sortedConnectionHashes.length) {
                                i = 0;
                            }
                        } while(i != pos);
                    }
                }
            }
        }

        return connectionIndexes[pos];
    }

}
