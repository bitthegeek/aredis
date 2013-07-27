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
import java.util.List;

class AredisConnectionShutDownManager extends Thread {

    private List<AsyncRedisConnection> connections;

    long shutDownTime;

    // Note that the waitingThread can be replaced with a new one till shutDownTime is past
    private static volatile AredisConnectionShutDownManager waitingThread;

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        AredisConnectionShutDownManager sm = AsyncRedisConnection.shutdownManager;
        if(sm == null) {
            synchronized(AredisConnectionShutDownManager.class) {
                sm = AsyncRedisConnection.shutdownManager;
                // Actually DCL is not required since there is only one Shutdown Hook
                if(sm == null) {
                    // Means the call is from the Shutdown Hook. Do initialization
                    shutDownTime = startTime + AsyncRedisConnection.MAX_SHUTDOWN_WAIT_MILLIS;
                    sm = this;
                    synchronized(this) {
                        AsyncRedisConnection.shutdownManager = sm;
                        waitingThread = this;
                        connections = new ArrayList<AsyncRedisConnection>();
                        connections.addAll(AsyncRedisConnection.openConnections);
                        int i, len = connections.size();
                        for(i = len; i > 0;) {
                            i--;
                            if(connections.get(i).okToShutdown(shutDownTime) == 0) {
                                connections.remove(i);
                                len--;
                            }
                        }
                    }
                }
            }
        }
        long waitTime;
        synchronized(sm) {
            do {
                long now = System.currentTimeMillis();
                if(sm.connections.size() > 0) {
                    waitTime = sm.shutDownTime - now;
                }
                else {
                    waitTime = 20 + startTime - now;
                }
                if(waitTime > 0) {
                    try {
                        sm.wait(waitTime);
                    } catch (InterruptedException e) {
                    }
                }
            } while(waitTime > 0);
            waitingThread = null;
        }
    }

    public synchronized void addConnection(AsyncRedisConnection con) {
        if(!connections.contains(con)) {
            connections.add(con);
        }
        if(waitingThread == null && shutDownTime > System.currentTimeMillis()) {
            waitingThread = new AredisConnectionShutDownManager();
            waitingThread.start();
        }
    }

    public synchronized void removeConnection(AsyncRedisConnection con) {
        connections.remove(con);
        if(connections.size() == 0) {
            notifyAll();
        }
    }
}
