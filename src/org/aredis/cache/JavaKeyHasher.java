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


/**
 * A basic Key Hasher which takes the java hashcode of the key and assigns the index got by the reminder when the
 * hashcode is divided by the number of servers.
 * @author Suresh
 *
 */
public class JavaKeyHasher implements KeyHasher {

    @Override
    public int getConnectionIndex(String key,
            AsyncRedisConnection[] connections, boolean useOnlyActive) {
        int i;
        int pos = 0;
        if(connections.length > 1) {
            if(useOnlyActive) {
                boolean useable[] = new boolean[connections.length];
                int statusCode = ShardedAsyncRedisClient.getUseableConnections(connections, useable);
                if(statusCode >= 0) {
                    pos = statusCode;
                }
                else {
                    int numUseableServers = -statusCode;
                    pos = key.hashCode() % numUseableServers;
                    if(numUseableServers < connections.length) {
                        i = pos + 1;
                        for(pos = 0; pos < connections.length; pos++) {
                            if(useable[pos]) {
                                i--;
                                if(i == 0) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            else {
                pos = key.hashCode() % connections.length;
            }
        }

        return pos;
    }

}
