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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.cache.AsyncHandler;
import org.aredis.cache.AsyncRedisConnection;
import org.aredis.util.SortedArray.IndexUpdater;

/**
 * Provides a base implementation of AsyncSocketTransport which provides connection status handling and a retry mechanism.
 * It maintains a common {@link ConnectionState} for all connections to the same host and port and issues only one retry for all connections.
 * @author suresh
 *
 */
public abstract class AbstractAsyncSocketTransport implements AsyncSocketTransport {

    private class RetryHandler implements AsyncHandler<ConnectionStatus>, Runnable {
        AsyncHandler<Boolean> connectHandler;

        Throwable exception;

        @Override
        public void completed(ConnectionStatus result, Throwable e) {
            if(result == ConnectionStatus.OK) {
                connectInternal(connectHandler);
            }
            else {
                exception = e;
                AsyncRedisConnection.bootstrapExecutor.execute(this);
            }
        }

        @Override
        public void run() {
            connectHandler.completed(false, exception);
        }
    }

    private static final Log log = LogFactory.getLog(AbstractAsyncSocketTransport.class);

    private final ConnectionState connectionState;

    protected final String host;

    protected final int port;

    protected final String connectionString;

    protected int serverIndex;

    protected AsyncSocketTransportFactory transportFactory;

    protected AsyncSocketTransportConfig config;

    /**
     * Last time this connection was used. Reads and writes of implementation should update this to current time.
     */
    protected long lastUseTime;

    /**
     * Last time a successful connection was made. This has to be updated by the implementation.
     */
    protected long lastConnectTime;

    /**
     * ConnectionStatus of this connection.
     */
    protected volatile ConnectionStatus connectionStatus;

    private RetryHandler retryHandler;

    /**
     * @param phost Server Host
     * @param pport Server Port
     */
    public AbstractAsyncSocketTransport(String phost, int pport) {
        host = phost.trim();
        port = pport;
        connectionString = host + ':' + port;
        ServerInfo t = ServerInfoComparator.findItem(this, new IndexUpdater() {
            @Override
            public void updateIndex(int index) {
                serverIndex = index;
            }
        });
        if (t != this) {
            serverIndex = t.getServerIndex();
        }
        connectionState = ConnectionState.getInstance(connectionString);
        connectionStatus = ConnectionStatus.CLOSED;
        retryHandler = new RetryHandler();
    }

    /**
     * Connect Method to be implemented by sub-class. The connect implementation in this class calls this to establish the socket connection.
     * @param handler handler to call when connection is completed
     */
    protected abstract void connectInternal(AsyncHandler<Boolean> handler);

    /**
     * Method to update the connection status. This is provided for the concrete sub-class to update the connection status. So this should be called with
     * DOWN whenever a Async socket read or write fails and should be called with OK when a connection succeeds.
     * @param status New Connection Status
     * @param e Exception in case the Connection Status is DOWN
     */
    protected void updateConnectionStatus(ConnectionStatus status, Throwable e) {
        if(status != ConnectionStatus.OK) {
            status = ConnectionStatus.DOWN;
        }
        if(connectionStatus != ConnectionStatus.DOWN && status == ConnectionStatus.DOWN) {
            log.warn("Connection DOWN for: " + this);
        }
        if(connectionStatus == ConnectionStatus.DOWN && status == ConnectionStatus.OK) {
            log.info("Connection UP AGAIN for: " + this);
        }
        connectionStatus = status;
        connectionState.updateStatus(status, e);
    }

    @Override
    public ConnectionStatus getStatus() {
        ConnectionStatus status = connectionStatus;
        if(status == ConnectionStatus.OK) {
            // Check if stale
            long idleTimeoutMillis = config.getMaxIdleTimeMillis();
            if(idleTimeoutMillis != 0 && System.currentTimeMillis() - lastUseTime >= idleTimeoutMillis) {
                status = ConnectionStatus.STALE;
            } else {
                // Return retry if there has been a down time since this connection was made
                long lastDownTime = connectionState.getLastDownTime();
                if (lastDownTime > 0 && lastConnectTime > 0 && lastDownTime >= lastConnectTime) {
                    status = ConnectionStatus.RETRY;
                }
            }
        }
        else {
            ConnectionStatus globalStatus = connectionState.checkRetry(null);
            if(globalStatus != ConnectionStatus.OK || status != ConnectionStatus.CLOSED) {
                status = globalStatus;
                if(status != ConnectionStatus.DOWN) {
                    status = ConnectionStatus.RETRY;
                }
            }
        }
        return status ;
    }

    @Override
    public void connect(AsyncHandler<Boolean> handler) {
        retryHandler.connectHandler = handler;
        ConnectionStatus status = connectionState.checkRetry(retryHandler);
        if(status == ConnectionStatus.OK || status == ConnectionStatus.RETRY) {
            connectInternal(handler);
        }
        else if(status == ConnectionStatus.DOWN) {
            retryHandler.completed(status, connectionState.getLastException());
        }
    }

    /**
     * Returns the transportFactory for this connection.
     * @return TransportFactory
     */
    public AsyncSocketTransportFactory getTransportFactory() {
        return transportFactory;
    }

    @Override
    public long getDownSince() {
        return connectionState.getDownSince();
    }

    @Override
    public long getRetryInterval() {
        return connectionState.getRetryInterval();
    }

    @Override
    public AsyncSocketTransportConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(AsyncSocketTransportConfig pconfig) {
        config = pconfig;
    }

    @Override
    public boolean isStale() {
        long idleTimeoutMillis = config.getMaxIdleTimeMillis();
        boolean stale = idleTimeoutMillis != 0 && System.currentTimeMillis() - lastUseTime >= idleTimeoutMillis;
        return stale;
    }

    /**
     * Returns a String representation containing the host and port.
     */
    public String toString() {
        return super.toString() + ": " + host + ":" + port;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getConnectionString() {
        return connectionString;
    }

    @Override
    public int getServerIndex() {
        return serverIndex;
    }

}
