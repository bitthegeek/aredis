/*
 * Copyright (C) 2013-2014 Suresh Mahalingam.  All rights reserved.
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

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.cache.RedisCommandInfo.CommandStatus;
import org.aredis.net.AsyncSocketTransport;
import org.aredis.net.AsyncSocketTransportConfig;
import org.aredis.net.ConnectionStatus;
import org.aredis.util.PeriodicTask;
import org.aredis.util.PeriodicTaskPoller;
import org.aredis.util.concurrent.SingleConsumerQueue;

/**
 * This is the pipelined asynchronous implementation of AsyncRedisClient that is the basis of aredis.
 * @author suresh
 *
 */
public class AsyncRedisConnection extends AbstractAsyncRedisClient {

    private class ConnectHandler implements AsyncHandler<Boolean> {
        @Override
        public void completed(Boolean success, Throwable e) {
            boolean isInfo = log.isInfoEnabled();
            boolean isDebug = log.isDebugEnabled();
            if(isInfo) {
                log.info("Trying to connect..");
            }
            if(success && e == null) {
                if(isInfo) {
                    log.info("Connect Successful");
                }
                resetOnConnect();
                RedisCommandList commandList;
                RedisCommandInfo[] commandInfos = {
                        new RedisCommandInfo(KEY_HANDLER, RedisCommand.CONFIG, "GET", "timeout")
                        };
                commandList = new RedisCommandList(commandInfos , queuedMultiCommands, connectCommandsResponseHander, false);
                commandList.isSyncCallback = true;
                try {
                    commandList.generateRequestData(con);
                } catch (IOException e1) {
                    log.error("Internal Error Generating CONFIG Request Data", e);
                }
                requestQueue.push(commandList);
                if(dbIndex > 0) {
                    commandInfos = new RedisCommandInfo[] {
                            new RedisCommandInfo(RedisCommand.SELECT, dbIndex)
                            };
                    commandList = new RedisCommandList(commandInfos , queuedMultiCommands, null, false);
                    try {
                        commandList.generateRequestData(con);
                    } catch (IOException e1) {
                        log.error("Internal Error Generating SELECT DB Request Data", e);
                    }
                    requestQueue.push(commandList);
                }
                if(!isCommonConnection && scriptStatuses.getStatusFlags().length > 0) {
                    try {
                        AsyncRedisConnection commonAredis = RedisServerWideData.getInstance(con).getCommonAredisConnection(null, 0);
                        RedisCommandInfo res = commonAredis.submitCommand(new RedisCommandInfo(RedisCommand.INFO, "server")).get();
                        String serverInfoStr = (String) res.getResult();
                        long serverStartTime = System.currentTimeMillis();
                        if (isDebug) {
                            log.debug("Trying to get uptime on re-connect using common aredis " + " Run Status " + res.getRunStatus() + " Server Info Str: " + serverInfoStr);
                        }
                        if (serverInfoStr != null) {
                            int startPos = serverInfoStr.indexOf("uptime_in_seconds");
                            if (startPos >= 0) {
                                startPos = serverInfoStr.indexOf(':', startPos);
                                if (startPos > 0) {
                                    int len = serverInfoStr.length();
                                    do {
                                        startPos++;
                                    } while (startPos < len && !Character.isDigit(serverInfoStr.charAt(startPos)));
                                    if (startPos < len) {
                                        int endPos = startPos;
                                        do {
                                            endPos++;
                                        } while (endPos < len && Character.isDigit(serverInfoStr.charAt(endPos)));
                                        long upTimeInMillis = Long.parseLong(serverInfoStr.substring(startPos, endPos)) * 1000;
                                        if (isDebug) {
                                            log.debug("GOT UPTIME: " + (upTimeInMillis / 1000) + " for " + con);
                                        }
                                        // Adding 1 second cushion
                                        serverStartTime = System.currentTimeMillis() - upTimeInMillis + 1000;
                                    }
                                }
                            }
                        }
                        scriptStatuses.clearLoadStatusesIfFirstStatusUpdateBefore(serverStartTime);
                    } catch (Exception ex) {
                        scriptStatuses.clearLoadStatuses();
                    }
                } else if(isDebug) {
                    log.debug("Skipping Uptime Check for Script on re-connect, isCommonConnection = " + isCommonConnection + " Length of Script Status Flags for this Server = " + scriptStatuses.getStatusFlags().length);
                }
            }
            processNextRequests();
        }
    }

    private class ConnectCommandsResponseHander implements AsyncHandler<RedisCommandInfo[]> {

        @Override
        public void completed(RedisCommandInfo[] results, Throwable e) {
            if(e == null && results[0].getRunStatus() == CommandStatus.SUCCESS) {
                try {
                    Object timeoutResults[] = (Object[]) results[0].getResult();
                    long serverTimeout = Integer.parseInt((String) timeoutResults[1]) * 1000L;
                    AsyncSocketTransportConfig config = con.getConfig();
                    long maxIdleTime = config.getMaxIdleTimeMillis();
                    if(serverTimeout > 0 && (maxIdleTime == 0 || serverTimeout < maxIdleTime)) {
                        config.setMaxIdleTimeMillis(serverTimeout - 500);
                    }
                }
                catch(Exception ex) {
                    log.error("Error setting Idle timeout from Server", ex);
                }
            }
        }

    }

    private class RequestHandler implements AsyncHandler<RedisCommandList> {
        private RedisCommandList commandList;

        @Override
        public void completed(RedisCommandList result, Throwable e) {
            if(result != null && e == null) {
                moveToResponseQueue(commandList);
            }
            else {
                sendFinalResponse(commandList, false, false);
            }
            processNextRequests();
        }
    }

    private class ResponseHandler implements AsyncHandler<RedisCommandList> {
        private RedisCommandList commandList;

        @Override
        public void completed(RedisCommandList result, Throwable e) {
            if(result == null || e != null) {
                isResponseErrored = true;
            }
            sendFinalResponse(commandList, !isResponseErrored, false);
            processNextResponses();
        }
    }

    private class FlushHandler implements AsyncHandler<Integer> {

        @Override
        public void completed(Integer result, Throwable e) {
            if(e == null) {
                flushCount++;
            }
            processNextRequests();
        }

    }

    private class ForceCloseThread extends Thread {
        private int delayMillis;

        public void run() {
            try {
                sleep(delayMillis);
                if(requestQueue.acquireIdle()) {
                    requestQueue.markIdle();
                }
            } catch (InterruptedException e) {
            }
        }
    }

    private class RequestQueueIdleListener implements SingleConsumerQueue.IdleListener<RedisCommandList> {

        private ForceCloseThread forceCloseThread;

        @Override
        public void beforeIdle(SingleConsumerQueue<RedisCommandList> q) {
            long st = shutdownTime;
            int timeToForceClose = okToShutdown(st);
            if(timeToForceClose == 0) {
                try {
                    con.close();
                } catch (IOException e) {
                    log.error("Error closing connection", e);
                }
                AredisConnectionShutDownManager sm = shutdownManager;
                if(sm != null) {
                    sm.removeConnection(AsyncRedisConnection.this);
                }
            }
            else if(timeToForceClose > 0 && forceCloseThread == null) {
                forceCloseThread.delayMillis = timeToForceClose + 100;
                forceCloseThread.setDaemon(true);
                forceCloseThread.start();
            }
        }

        @Override
        public void afterAcquireIdle(SingleConsumerQueue<RedisCommandList> q) {
            if(q.size() > 0) {
                AredisConnectionShutDownManager sm = shutdownManager;
                if(sm != null) {
                    sm.addConnection(AsyncRedisConnection.this);
                }
            }
        }

    }

    private class StartRequestProcessingTask extends Thread {
        @Override
        public void run() {
            processNextRequests();
        }
    }

    private class CloseStaleConnectionTask extends Thread {
        @Override
        public void run() {
            if(con.isStale() && requestQueue.acquireIdle()) {
                processNextRequests();
            }
        }
    }

    private class StartResponseProcessingTask extends Thread {
        @Override
        public void run() {
            processNextResponses();
        }
    }

    private class AredisPeriodicTask implements PeriodicTask {

        @Override
        public void doPeriodicTask(long now) {
            if(con.isStale()) {
                bootstrapExecutor.execute(closeStaleConnectionTask);
            }
        }

    }

    private static final Log log = LogFactory.getLog(AsyncRedisConnection.class);

    /**
     * This {@link DataHandler} is an instance of {@link StringHandler}. It is also used to serialize keys and other non-value parameters to the Redis Commands.
     */
    public static final DataHandler KEY_HANDLER = new StringHandler();

    /**
     * Default Per Connection Class Descriptor Storage used by OPTI_JAVA_HANDLER.
     */
    public static final ClassDescriptorStorageFactory DEFAULT_CLASS_DESCRIPTOR_STORAGE_FACTORY = new PerConnectionRedisClassDescriptorStorageFactory(null);

    /**
     * This {@link DataHandler} is an instance of {@link JavaHandler}. It is configured to optimize object storage by storing the class descriptors for the classes separately.
     */
    public static final DataHandler OPTI_JAVA_HANDLER = new JavaHandler(DEFAULT_CLASS_DESCRIPTOR_STORAGE_FACTORY, true);

    /**
     * This {@link DataHandler} is an instance of {@link JavaHandler} with normal Object serialization.
     */
    public static final DataHandler JAVA_HANDLER = new JavaHandler();

    /**
     * Identical to OPTI_JAVA_HANDLER but without compression.
     */
    public static final DataHandler OPTI_JAVA_HANDLER_NO_COMPRESS = new JavaHandler(DEFAULT_CLASS_DESCRIPTOR_STORAGE_FACTORY, true);

    /**
     * Identical to JAVA_HANDLER but without compression.
     */
    public static final DataHandler JAVA_HANDLER_NO_COMPRESS = new JavaHandler();

    /**
     * The Default Data Handler if none is configured. It is initialized to OPTI_JAVA_HANDLER by default.
     */
    public static final DataHandler DEFAULT_HANDLER = OPTI_JAVA_HANDLER;

    /**
     * Default value of max pipeline size to restrict the number of commands in pipeline after which the request processing is suspended till the pipeline size reduces.
     */
    public static final int DEFAULT_MAX_PIPELINE_SIZE = 1000;

    /**
     * Executor used to initialize the pipeline when there is no NIO request listener. It is also used as the executor if for completionHandlers if none is supplied which is not a good thing since there are only 10 threads.
     */
    public static final ExecutorService bootstrapExecutor;

    public static final Thread shutDownHook = new AredisConnectionShutDownManager();

    public static volatile int MAX_SHUTDOWN_WAIT_MILLIS = 5000;

    static {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10, 10, 15, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        bootstrapExecutor = threadPoolExecutor;
        JavaHandler dh = (JavaHandler) OPTI_JAVA_HANDLER_NO_COMPRESS;
        dh.setObjectCompressionThreshold(-1);
        dh.setStringCompressionThreshold(-1);
        dh = (JavaHandler) JAVA_HANDLER_NO_COMPRESS;
        dh.setObjectCompressionThreshold(-1);
        dh.setStringCompressionThreshold(-1);
        Runtime.getRuntime().addShutdownHook(shutDownHook);
    }

    int maxPending;

    int flushCount;

    private AsyncSocketTransport con;

    private AsyncSocketTransportConfig savedConfig;

    SingleConsumerQueue<RedisCommandList> requestQueue;

    SingleConsumerQueue<RedisCommandList> responseQueue;

    /**
     * Task executor for invoking completionHandlers for Async commands.
     */
    protected Executor taskExecutor;

    private Queue<RedisCommandInfo> queuedMultiCommands;

    private int currentDbIndex;

    private int dbIndex;

    private DataHandler dataHandler;

    private FlushHandler flushHandler;

    private int accumulatedMicros;

    private AtomicInteger pipelineSize;

    private ConnectHandler connectHandler;

    private ConnectCommandsResponseHander connectCommandsResponseHander;

    private RequestHandler requestHandler;

    private StartRequestProcessingTask startRequestProcessingTask;

    private StartResponseProcessingTask startResponseProcessingTask;

    private CloseStaleConnectionTask closeStaleConnectionTask;

    private AredisPeriodicTask periodicTask;

    private ResponseHandler responseHandler;

    private int maxPipelineSize;

    private int resumePipelineSize;

    private Lock pipelineLock;

    private Condition pipelineCondition;

    private volatile boolean pipelineFull;

    private boolean isResponseErrored;

    private volatile long shutdownTime;

    private ConnectionType connectionType;

    ScriptStatuses scriptStatuses;

    boolean hasSelectCommands;

    boolean isBorrowed;

    boolean isCommonConnection;

    static ThreadLocal<Boolean> insideCallback;

    static Vector<AsyncRedisConnection> openConnections = new Vector<AsyncRedisConnection>();

    static volatile AredisConnectionShutDownManager shutdownManager;

    /**
     * Constructor to create an AsyncRedisConnection of the given connection type.
     * It is preferable to use {@link AsyncRedisFactory} than this constructor.
     * @param pcon Async Socket Transport implementation
     * @param pdbIndex Redis DB index
     * @param ptaskExecutor Task Executor to use for Asyc APIs. Can be null if you do not use Async APIs with completion handler
     * @param pconnectionType ConnectionType indicating type of connection. A normal SHARED connection type is for Shared usage and the WATCH command is not allowed.
     * If the ConnectionType is BORROWED WATCH is allowed but not blocking commands like BLPOP. If the ConnectionType is STANDALONE all commands except SUBSCRIBE are allowed.
     */
    public AsyncRedisConnection(AsyncSocketTransport pcon, int pdbIndex, Executor ptaskExecutor, ConnectionType pconnectionType) {
        con = pcon;
        dbIndex = pdbIndex;
        AsyncSocketTransportConfig config = con.getConfig();
        savedConfig = config;
        con.setConfig(config.clone());
        taskExecutor = ptaskExecutor;
        connectionType = pconnectionType;
        if(pconnectionType == null) {
            connectionType = ConnectionType.SHARED;
        }
        queuedMultiCommands = new LinkedList<RedisCommandInfo>();
        requestQueue = new SingleConsumerQueue<RedisCommandList>(2000);
        RequestQueueIdleListener idleListener = new RequestQueueIdleListener();
        requestQueue.setIdleListener(idleListener);
        responseQueue = new SingleConsumerQueue<RedisCommandList>();
        pipelineSize = new AtomicInteger();
        pipelineLock = new ReentrantLock();
        pipelineCondition = pipelineLock.newCondition();
        dataHandler = AsyncRedisConnection.DEFAULT_HANDLER;
        setMaxPipelineSize(DEFAULT_MAX_PIPELINE_SIZE);
        connectHandler = new ConnectHandler();
        connectCommandsResponseHander = new ConnectCommandsResponseHander();
        requestHandler = new RequestHandler();
        responseHandler = new ResponseHandler();
        flushHandler = new FlushHandler();
        startRequestProcessingTask = new StartRequestProcessingTask();
        startResponseProcessingTask = new StartResponseProcessingTask();
        closeStaleConnectionTask = new CloseStaleConnectionTask();
        periodicTask = new AredisPeriodicTask();
        scriptStatuses = RedisServerWideData.getInstance(con).getScriptStatuses();
        insideCallback = new ThreadLocal<Boolean>();
    }

    /**
     * Constructor to create a SHARED AsyncRedisConnection.
     * It is preferable to use {@link AsyncRedisFactory} than this constructor.
     * @param pcon Async Socket Transport implementation
     * @param pdbIndex Redis DB index
     * @param ptaskExecutor Task Executor to use for Asyc APIs. Can be null if you do not use Async APIs with completion handler
     */
    public AsyncRedisConnection(AsyncSocketTransport pcon, int pdbIndex, Executor ptaskExecutor) {
        this(pcon, pdbIndex, ptaskExecutor, ConnectionType.SHARED);
    }

    private void resetOnConnect() {
        pipelineSize.set(0);
        queuedMultiCommands.clear();
        isResponseErrored = false;
        accumulatedMicros = 0;
        currentDbIndex = 0;
        AsyncSocketTransportConfig config = con.getConfig();
        config.setMaxIdleTimeMillis(savedConfig.getMaxIdleTimeMillis());
        PeriodicTaskPoller periodicTaskPoller = PeriodicTaskPoller.getInstance();
        periodicTaskPoller.addTask(periodicTask);
    }

    private void moveToResponseQueue(RedisCommandList processedCommandList) {
        if(processedCommandList != null) {
            boolean startResponseProcessing = responseQueue.add(processedCommandList, true);
            int qSize = responseQueue.size();
            if(qSize > maxPending) {
                maxPending = qSize;
            }
            if(startResponseProcessing) {
                bootstrapExecutor.execute(startResponseProcessingTask);
            }
        }
    }

    private boolean processNextRequest() {
        boolean isInfo = log.isInfoEnabled();
        boolean isSyncSend = false;
        RedisCommandList commandList;
        ConnectionStatus connectionStatus = con.getStatus();
        if(connectionStatus == ConnectionStatus.CLOSED ||
           connectionStatus == ConnectionStatus.STALE ||
           connectionStatus == ConnectionStatus.RETRY) {
            if(requestQueue.size() == 0 && responseQueue.size() == 0 && con.isStale()) {
                try {
                    con.close();
                    if(isInfo) {
                        log.info("CLOSED STALE CONNECTION + " + this);
                    }
                    PeriodicTaskPoller periodicTaskPoller = PeriodicTaskPoller.getInstance();
                    periodicTaskPoller.removeTask(periodicTask);
                    openConnections.remove(this);
                } catch (IOException e) {
                    log.error("Error closing idle connection " + con, e);
                }
                requestQueue.markIdle();
                if(requestQueue.size() > 0 && requestQueue.acquireIdle()) {
                    isSyncSend = processNextRequest();
                }
            }
            else {
                boolean idle = false;
                if(responseQueue.size() > 0) {
                    requestQueue.markIdle();
                    if(responseQueue.size() == 0) {
                        idle = !requestQueue.acquireIdle();
                    }
                }
                if(!idle) {
                    openConnections.add(this);
                    con.connect(connectHandler);
                }
                if(isInfo) {
                    log.info("IDLING " + this);
                }
            }
        }
        else if(connectionStatus == ConnectionStatus.OK) {
            long timeoutMicros = accumulatedMicros;
            if(timeoutMicros > 200) {
                // Grows at 1/4th rate after 200 micro secs
                timeoutMicros = 200 + (timeoutMicros - 200) >> 2;
            }
            if(timeoutMicros > 10000) {
                // Max 10 ms dely
                timeoutMicros = 10000;
            }
            if(timeoutMicros < 100) {
                timeoutMicros = 0;
            }
            boolean flushPending = false;
            if(con.requiresFlush()) {
                flushPending = true;
            }
            commandList = requestQueue.remove(timeoutMicros, !flushPending);
            if(commandList != null) {
                if(connectionType == ConnectionType.SHARED) {
                    int requiredDbIndex = commandList.checkSelectCommands(dbIndex, currentDbIndex);
                    if(requiredDbIndex >= 0 && requiredDbIndex != currentDbIndex) {
                        requestQueue.push(commandList);
                        commandList = new RedisCommandList(new RedisCommandInfo(RedisCommand.SELECT, String.valueOf(requiredDbIndex)), queuedMultiCommands, null, false);
                        try {
                            commandList.generateRequestData(con);
                        } catch (IOException e) {
                            log.error("Internal Error Generating SELECT DB Request Data", e);
                        }
                        commandList.finalDbIndex = requiredDbIndex;
                    }
                }
                else if(connectionType == ConnectionType.BORROWED && isBorrowed && commandList.hasSelectCommands) {
                    hasSelectCommands = true;
                }
                // Approximate assumption that if commandList was retrieved there was no wait
                // Which may not be true if the item was got while waiting.
                // Cannot accumulate more than 1s.
                if(accumulatedMicros < 1000000) {
                    accumulatedMicros += 4;
                }
                requestHandler.commandList = commandList;
                isSyncSend = commandList.sendRequest(con, requestHandler, scriptStatuses);
                if(connectionType == ConnectionType.SHARED) {
                    currentDbIndex = commandList.finalDbIndex;
                }
                if(isSyncSend) {
                    moveToResponseQueue(commandList);
                }
            }
            else {
                if(timeoutMicros >= 100) {
                    accumulatedMicros -= timeoutMicros;
                }
                if(flushPending) {
                    con.flush(flushHandler);
                }
            }
        }
        else {
            // connectionStatus == ConnectionStatus.DOWN
            // Send immediate skipped response
            commandList = requestQueue.remove(0, true);
            if(commandList != null) {
                isSyncSend = true;
                sendFinalResponse(commandList, false, false);
            }
        }

        return isSyncSend;
    }

    private void processNextRequests() {
        boolean isSyncSend;
        do {
            isSyncSend = processNextRequest();
        } while(isSyncSend);
    }

    void waitOnFullPipeline(long timeoutMicros) {
        if (timeoutMicros > 0) {
            try {
                pipelineFull = true;
                pipelineLock.lock();
                try {
                    pipelineCondition.await(timeoutMicros, TimeUnit.MICROSECONDS);
                } catch (InterruptedException e) {
                }
            } finally {
                pipelineLock.unlock();
            }
        }
    }

    void updatePipelineSizeForResponse(RedisCommandList processedCommandList) {
        if(pipelineSize.addAndGet(-processedCommandList.commandInfos.length) <= resumePipelineSize && pipelineFull) {
            try {
                pipelineLock.lock();
                pipelineFull = false;
                pipelineCondition.signalAll();
            } finally {
                pipelineLock.unlock();
            }
            /*
            if (requestQueue.size() > 0 && requestQueue.acquireIdle()) {
                bootstrapExecutor.execute(startRequestProcessingTask);
            }
            */
        }
    }

    private void sendFinalResponse(RedisCommandList processedCommandList, boolean issuccess, boolean forceCallbackViaExecutor) {
        if(issuccess) {
            processedCommandList.updateScriptStatuses(scriptStatuses);
        }
        Executor executor = taskExecutor;
        if(executor == null) {
            executor = bootstrapExecutor;
        }
        processedCommandList.sendFinalResponse(this, executor, forceCallbackViaExecutor);
    }

    private void processNextResponses() {
        boolean isSyncReceive;
        long timeoutMicros = 1000;
        if(isResponseErrored) {
            timeoutMicros = 0;
        }
        do {
            RedisCommandList commandList = responseQueue.remove(timeoutMicros, true);
            if(commandList == null) {
                if(requestQueue.size() > 0) {
                    if(requestQueue.acquireIdle()) {
                        bootstrapExecutor.execute(startRequestProcessingTask);
                    }
                }
                else {
                    long st = shutdownTime;
                    // The below acquire and release is done to close the connection if shutting down
                    // con should not be closed outside idle listener as a new request may
                    // arrive concurrently before sutdown delay
                    if(st > 0 && requestQueue.acquireIdle()) {
                        requestQueue.markIdle();
                        if(requestQueue.size() > 0 && requestQueue.acquireIdle()) {
                            bootstrapExecutor.execute(startRequestProcessingTask);
                        }
                    }
                }
                break;
            }
            if(isResponseErrored) {
                isSyncReceive = true;
                sendFinalResponse(commandList, false, false);
            }
            else {
                responseHandler.commandList = commandList;
                isSyncReceive = commandList.receiveResponse(con, responseHandler);
                if(isSyncReceive) {
                    sendFinalResponse(commandList, true, false);
                }
            }
        } while(isSyncReceive);
    }

    private void submitCommands(RedisCommandList commandList) {
        int i;
        long st = shutdownTime;
        if(st > 0 && System.currentTimeMillis() > st) {
            throw new IllegalStateException(this.toString() + " Cannot accepot commands after shutdown time.");
        }
        if(connectionType == ConnectionType.BORROWED && !isBorrowed) {
            throw new IllegalStateException(this.toString() + ": ConnetionType is Borrowed but the connection has not been borrowed from a pool or has already been returned to pool");
        }
        String msg = commandList.validateCommandList(connectionType);
        if(msg != null) {
            throw new IllegalArgumentException(this.toString() + ": " + msg);
        }
        if(con.getStatus() == ConnectionStatus.DOWN) {
            sendFinalResponse(commandList, false, true);
        }
        else {
            boolean processRequest = false;
            try {
                RedisCommandInfo[] commandInfos = commandList.commandInfos;
                for(i = 0; i < commandInfos.length; i++) {
                    RedisCommandInfo commandInfo = commandInfos[i];
                    commandInfo.serverInfo = con;
                    if(commandInfo.dataHandler == null) {
                        commandInfo.dataHandler = commandInfo.getCommand().isUseKeyHandlerAsDefaultDataHandler() ? KEY_HANDLER : dataHandler;
                    }
                }
                commandList.generateRequestData(con);
                processRequest = true;
            } catch (IOException e) {
                log.error("Error Generating Request Data", e);
                sendFinalResponse(commandList, false, true);
            }
            if(processRequest) {
                int pipelineThreshold = maxPipelineSize;
                if (insideCallback.get() != null) {
                    // Try and avoid blocking in Callback because it again uses up the
                    // executor Thread Pool further slowing down command processing making
                    // it cyclic
                    pipelineThreshold = pipelineThreshold + 1000;
                }
                int currentPipelineSize = pipelineSize.get();
                if(currentPipelineSize > pipelineThreshold) {
                    long waitTimeMicros = 10000;
                    if (currentPipelineSize > pipelineThreshold + 500) {
                        waitTimeMicros = 100000;
                    }
                    waitOnFullPipeline(waitTimeMicros);
                }
                boolean startRequestProcessing = requestQueue.add(commandList, true);
                if(startRequestProcessing) {
                    bootstrapExecutor.execute(startRequestProcessingTask);
                }
                pipelineSize.addAndGet(commandList.commandInfos.length);
            }
        }
    }

    /**
     * Closes the connection if it has been idle beyond the configured idle timeout or the Redis Server timeout.
     * A connection will be automatically re-established on the next command.
     */
    public void closeStaleConnection() {
        if(con.isStale() && requestQueue.acquireIdle()) {
            bootstrapExecutor.execute(startRequestProcessingTask);
        }
    }

    /**
     * Shutdown the connection.
     * The connection is closed as soon as it is idle and re-opened whenever a command arrives till delayMillis milliseconds.
     * After that new commands are rejected. This method however is currently not the preferred way to shutdown because the socket
     * connections close automatically once the JVM is eligible for shutdown.
     * Idle connections are closed after the configure timeout in any case.
     * @param delayMillis Max delay till which to support commands
     */
    public void shutdown(long delayMillis) {
        long st = shutdownTime;
        if(delayMillis < 0) {
            throw new IllegalArgumentException("delayMillis " + delayMillis + " is negative");
        }
        if(st > 0) {
            log.warn(this.toString() + ": Ignoring duplicate shutdown call");
        }
        else {
            st = System.currentTimeMillis() + delayMillis;
            shutdownTime = st;
            // Acquire and release request Q if empty so that connection is closed
            if(requestQueue.acquireIdle()) {
                requestQueue.markIdle();
                if(requestQueue.size() > 0 && requestQueue.acquireIdle()) {
                    bootstrapExecutor.execute(startRequestProcessingTask);
                }
            }
            PeriodicTaskPoller periodicTaskPoller = PeriodicTaskPoller.getInstance();
            periodicTaskPoller.removeTask(periodicTask);
        }
    }

    @Override
    public Future<RedisCommandInfo[]> submitCommands(RedisCommandInfo commands[], AsyncHandler<RedisCommandInfo[]> completionHandler, boolean requireFutureResult, boolean isSyncCallback) {
        RedisCommandList commandList = new RedisCommandList(commands, queuedMultiCommands, completionHandler, requireFutureResult);
        if(isSyncCallback) {
            commandList.isSyncCallback = true;
        }
        submitCommands(commandList);
        return commandList.futureResults;
    }

    @Override
    public Future<RedisCommandInfo> submitCommand(RedisCommandInfo command, AsyncHandler<RedisCommandInfo> completionHandler, boolean requireFutureResult, boolean isSyncCallback) {
        RedisCommandList commandList = new RedisCommandList(command, queuedMultiCommands, completionHandler, requireFutureResult);
        if(isSyncCallback) {
            commandList.isSyncCallback = true;
        }
        submitCommands(commandList);
        return commandList.futureResult;
    }

    /**
     * Returns the Async Socket Transport used
     * @return Async Socket Transport used
     */
    public AsyncSocketTransport getConnection() {
        return con;
    }

    /**
     * Returns the DB Index of the connection
     * @return DB Index
     */
    public int getDbIndex() {
        return dbIndex;
    }

    /**
     * The default {@link DataHandler} for commands
     * @return Default Data handler
     */
    public DataHandler getDataHandler() {
        return dataHandler;
    }

    /**
     * Sets the default {@link DataHandler} for commands
     * @param pdataHandler Data Handler to set
     */
    public void setDataHandler(DataHandler pdataHandler) {
        if(pdataHandler != null) {
            dataHandler = pdataHandler;
        }
    }

    /**
     * Max Pipeline Size
     * @return Max Pipeline Size beyond which the request processing is suspended
     */
    public int getMaxPipelineSize() {
        return maxPipelineSize;
    }

    /**
     * Sets the max number of commands pending for response in the Redis Server.
     * This includes the commands in the local Q. Once this limit is reached the submitCommand calls are
     * delayed upto a max of 100 ms for the pipeline size to come down by 100 or more. If the value supplied is
     * less than 200 the pipeline size is taken as 200.
     * @param pmaxPipelineSize max number of commands allowed in Redis Server pipeline
     */
    public void setMaxPipelineSize(int pmaxPipelineSize) {
        if(pmaxPipelineSize < 200) {
            pmaxPipelineSize = 200;
        }
        int resumeSize = pmaxPipelineSize - 100;
        maxPipelineSize = pmaxPipelineSize;
        resumePipelineSize = resumeSize;
    }

    int okToShutdown(long st) {
        int timeToForceClose = -1;
        if(st > 0 && requestQueue.size() == 0 && responseQueue.size() == 0 && (queuedMultiCommands.size() == 0 || (timeToForceClose = (int) (st - System.currentTimeMillis() + 1900)) <= 0)) {
            timeToForceClose = 0;
        }

        return timeToForceClose;
    }

    /**
     * Method to change the size of the bootstrapExecutor
     * @param poolSize New Pool size
     */
    public static void setBootstrapExecutorPoolSize(int poolSize) {
        synchronized(bootstrapExecutor) {
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) bootstrapExecutor;
            int currentPoolSize = threadPoolExecutor.getCorePoolSize();
            if(poolSize > currentPoolSize) {
                threadPoolExecutor.setMaximumPoolSize(poolSize);
                threadPoolExecutor.setCorePoolSize(poolSize);
            }
            else if(poolSize < currentPoolSize) {
                threadPoolExecutor.setCorePoolSize(poolSize);
                threadPoolExecutor.setMaximumPoolSize(poolSize);
            }
        }
    }

}
