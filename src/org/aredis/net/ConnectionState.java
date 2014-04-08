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

package org.aredis.net;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.cache.AsyncHandler;

/**
 * Captures the Connection State for all AsyncSocketTransports connected to a given host and port. Please note that
 * this is an internal class only relevant if you want to add a new {@link AsyncSocketTransport}.
 * @author suresh
 *
 */
public class ConnectionState {
    /**
     * The starting Retry interval in milliseconds
     */
    public static final long INIT_RETRY_INTERVAL = 25;

    /**
     * The default value of maxRetryInterval. Default is 2 minutes.
     */
    public static final long DEFAULT_MAX_RETRY_INTERVAL = 120000;

    private static final Log log = LogFactory.getLog(ConnectionState.class);

    /**
     * max retry interval for this server.
     */
    public volatile long maxRetryInterval = DEFAULT_MAX_RETRY_INTERVAL;

    private volatile static Map<String, ConnectionState> serverMap;

    private long retryInterval;

    private long downSince;

    private volatile long lastDownTime;

    private long lastRetryTime;

    private Throwable lastException;

    private List<AsyncHandler<ConnectionStatus>> callBacks;

    /**
     * Creates a new ConnectionState.
     */
    public ConnectionState() {
        callBacks = new ArrayList<AsyncHandler<ConnectionStatus>>(4);
        retryInterval = INIT_RETRY_INTERVAL;
    }

    /**
     * Gets the connection state for the given server. Returns a common instance for a given serverKey.
     * @param serverKey Server Key in the form host:port
     * @return Connection State for the server
     */
    public static ConnectionState getInstance(String serverKey) {
        ConnectionState connectionState = null;
        Map<String, ConnectionState> sMap = serverMap;
        if(sMap != null) {
            connectionState = sMap.get(serverKey);
        }
        if(connectionState == null) {
            synchronized(ConnectionState.class) {
                sMap = serverMap;
                if(sMap != null) {
                    connectionState = sMap.get(serverKey);
                }
                if(connectionState == null) {
                    connectionState = new ConnectionState();
                    if(sMap == null) {
                        sMap = new HashMap<String, ConnectionState>();
                    }
                    else {
                        sMap = new HashMap<String, ConnectionState>(sMap);
                    }
                    sMap.put(serverKey, connectionState);
                    serverMap = sMap;
                }
            }
        }

        return connectionState;
    }

    /**
     * This method is to be called when the getStatus method returns RETRY. If this method returns RETRY then the caller should try
     * to connect and update the result of the attempt as DOWN or OK by calling updateStatus. RETRY is returned for only one of the
     * callers for a given server. The others get null as a response and are called back with the status via updateStatus.
     * @param callBack Callback to be called with the connection status if another connection is making a retry on the same server.
     * @return null if another connection is making a RETRY in which case a call is made on the callBack with the status. RETRY indicating
     * the caller has to try to connect. DOWN to indicate that the conneciton is DOWN.
     */
    public synchronized ConnectionStatus checkRetry(AsyncHandler<ConnectionStatus> callBack) {
        ConnectionStatus status = ConnectionStatus.OK;
        if(downSince > 0) {
            status = ConnectionStatus.DOWN;
            long now = System.currentTimeMillis();
            if(now - downSince >= retryInterval) {
                status = null;
                if(now - lastRetryTime >= retryInterval) {
                    status = ConnectionStatus.RETRY;
                    if(callBack != null) {
                        lastRetryTime = now + 30000; // Next caller will get null status till 30 seconds
                        if(retryInterval < 1000) {
                            retryInterval *= 10;
                            if(retryInterval > 1000) {
                                retryInterval = 1000;
                            }
                        }
                        else {
                            retryInterval *= 2;
                        }
                        if(retryInterval > maxRetryInterval) {
                            retryInterval = maxRetryInterval;
                        }
                    }
                }
                else if(callBack != null) {
                    callBacks.add(callBack);
                }
            }
        }

        return status;
    }

    /**
     * Method called to update the connectionStatus after a successful connection or if an error occurs in connection.
     * @param status ConnectionStatus indicating connection status
     * @param exception Exception which was thrown
     */
    public synchronized void updateStatus(ConnectionStatus status, Throwable exception) {
        if(status == ConnectionStatus.OK) {
            downSince = 0;
            retryInterval = INIT_RETRY_INTERVAL;
        }
        else {
            long now = System.currentTimeMillis();
            downSince = now;
            lastDownTime = now;
            lastRetryTime = now;
            status = ConnectionStatus.DOWN;
            lastException = exception;
        }
        for(AsyncHandler<ConnectionStatus> callBack : callBacks) {
            try {
                callBack.completed(status, null);
            }
            catch(Exception e) {
                log.error("Error In Connection Status Callback, Ignoring", e);
            }
        }
        callBacks.clear();
    }

    /**
     * The last exception for which updateStatus call.
     * @return last exception
     */
    public synchronized Throwable getLastException() {
        return lastException;
    }

    /**
     * Current Retry Interval when retrying broken connections.
     * @return retry interval in milliseconds
     */
    public synchronized long getRetryInterval() {
        return retryInterval;
    }

    /**
     * Milliseconds time when the connection was found to be down.
     * @return milliseconds time when the connection was last down
     */
    public synchronized long getDownSince() {
        return downSince;
    }

    /**
     * Milliseconds time when the connection was found to be down. This is not reset on re-connection.
     * The connection is re-tried if there was a downtime since a connection was made.
     * @return Last time when one of the AsyncRedisConnection's had a downtime with this server
     */
    public long getLastDownTime() {
        return lastDownTime;
    }
}
