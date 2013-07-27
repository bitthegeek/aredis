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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.io.RedisConstants;
import org.aredis.messaging.RedisSubscription;
import org.aredis.net.AsyncSocketTransport;
import org.aredis.net.AsyncSocketTransportConfig;
import org.aredis.net.ConnectionStatus;
import org.aredis.util.concurrent.SingleConsumerQueue;

/**
 * This is the base class used by {@link RedisSubscription}. RedisSubscription call the submitCommand method for its subscribe and unsubcribe callse.
 * This is an internal class.
 * @author suresh
 *
 */
public class RedisSubscriptionConnection {

    /**
     * Different Subscribe Unsubscribe Command Types.
     * @author suresh
     *
     */
    public static enum CommandType {SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, MESSAGE, PMESSAGE}

    private Object [] extractResponseInfo(RedisRawResponse mbResponseData[]) {
        Object results[] = new Object[mbResponseData.length];
        int i = 0;
        String commandTypeStr = new String((byte[]) mbResponseData[i].getResult(), RedisConstants.UTF_8_CHARSET).toUpperCase();
        CommandType commandType = CommandType.valueOf(commandTypeStr);
        results[i] = commandType;
        i++;
        byte[] resultBytes = (byte []) mbResponseData[i].getResult();
        if(resultBytes != null) {
            results[i] = new String(resultBytes, RedisConstants.UTF_8_CHARSET);
        }
        i++;
        switch(commandType) {
          case SUBSCRIBE:
          case UNSUBSCRIBE:
          case PSUBSCRIBE:
          case PUNSUBSCRIBE:
            results[i] = (Integer) Integer.parseInt((String) mbResponseData[i].getResult());
            i++;
            break;
          case PMESSAGE:
            results[i] = new String((byte []) mbResponseData[i].getResult(), RedisConstants.UTF_8_CHARSET);
            i++;
          case MESSAGE:
            results[i] = mbResponseData[i].getResult();
            break;
          default:
            break;
        }

        return results;
    }

    private class ConnectHandler implements AsyncHandler<Boolean> {
        @Override
        public void completed(Boolean success, Throwable e) {
            boolean isDebug = log.isDebugEnabled();
            if(isDebug) {
                log.debug("Trying to connect..");
            }
            if(success && e == null) {
                if(isDebug) {
                    log.debug("CONNECT SUCCESSFUL");
                }
                resetOnConnect();
                if(dbIndex > 0) {
                    RedisCommandInfo commandInfo = new RedisCommandInfo(RedisCommand.SELECT, dbIndex);
                    RedisCommandObject commandObject = new RedisCommandObject(commandInfo, null);
                    try {
                        commandObject.generateRequestData(con);
                        requestQueue.push(commandObject);
                    } catch (IOException e1) {
                        log.error("Internal Error Generating SELECT DB Request Data", e);
                    }
                }
                for(String channel : patternSubscriptionMap.keySet()) {
                    RedisCommandInfo commandInfo = new RedisCommandInfo(RedisCommand.PSUBSCRIBE, channel);
                    RedisCommandObject commandObject = new RedisCommandObject(commandInfo, null);
                    try {
                        commandObject.generateRequestData(con);
                        requestQueue.push(commandObject);
                    } catch (IOException e1) {
                        log.error("Internal Error Generating PSUBSCRIBE Request Data", e);
                    }
                }
                for(String channel : subscriptionMap.keySet()) {
                    RedisCommandInfo commandInfo = new RedisCommandInfo(RedisCommand.SUBSCRIBE, channel);
                    RedisCommandObject commandObject = new RedisCommandObject(commandInfo, null);
                    try {
                        commandObject.generateRequestData(con);
                        requestQueue.push(commandObject);
                    } catch (IOException e1) {
                        log.error("Internal Error Generating SUBSCRIBE Request Data", e);
                    }
                }
            }
            if(e != null) {
                e.printStackTrace();
            }
            processNextRequests();
        }
    }

    private class RequestHandler implements AsyncHandler<Integer> {
        @Override
        public void completed(Integer result, Throwable e) {
            // System.out.println("ENTERED REQUEST HANDLER");
            boolean processNext = true;
            if(processNext) {
                processNextRequests();
            }
        }
    }

    private class RequestQueueIdleListener implements SingleConsumerQueue.IdleListener<RedisCommandObject> {

        @Override
        public void beforeIdle(SingleConsumerQueue<RedisCommandObject> q) {
            if(isShutDown && requestQueue.size() == 0 && responseQueue.size() == 0) {
                try {
                    con.close();
                } catch (IOException e) {
                    log.error("Error closing connection", e);
                }
            }
        }

        @Override
        public void afterAcquireIdle(SingleConsumerQueue<RedisCommandObject> q) {
        }

    }

    private class SendMessageTask implements Runnable {
        SubscriptionInfo subscriptionInfo;

        String channelName;

        byte data[];

        public SendMessageTask(SubscriptionInfo psubscriptionInfo, String pchannelName, byte pdata[]) {
            subscriptionInfo = psubscriptionInfo;
            channelName = pchannelName;
            data = pdata;
        }

        @Override
        public void run() {
            Object message = null;
            boolean isDataError = false;
            try {
                DataHandler dataHandler = subscriptionInfo.getDataHandler();
                if(dataHandler == null) {
                    dataHandler = AsyncRedisConnection.OPTI_JAVA_HANDLER;
                }
                message = dataHandler.deserialize(subscriptionInfo.getMetaData(), data, 0, data.length, con);
            }
            catch(Exception e) {
                isDataError = true;
                log.error("Error Deserializing Message for channel: " + channelName, e);
            }
            if(!isDataError) {
                try {
                    subscriptionInfo.getListener().receive(subscriptionInfo, channelName, message);
                } catch (Exception e) {
                    log.error("Error in messageListener for channel: " + channelName + " for message " + message, e);
                }
            }
        }

    }

    private void processMessage(CommandType commandType, Object[] responses) {
        int i = 1;
        String subscriptionChannel = (String) responses[i++];
        String channelName = subscriptionChannel;
        Map<String, SubscriptionInfo> sm = subscriptionMap;
        if(commandType == CommandType.PMESSAGE) {
            sm = patternSubscriptionMap;
            channelName = (String) responses[i++];
        }
        SubscriptionInfo subscriptionInfo = sm.get(subscriptionChannel);
        byte data[] = (byte []) responses[i++];
        SendMessageTask sendMessageTask = new SendMessageTask(subscriptionInfo, channelName, data);
        Executor executor = subscriptionInfo.executor;
        if(executor == null) {
            executor = taskExecutor;
        }
        if(executor == null) {
            executor = AsyncRedisConnection.bootstrapExecutor;
        }
        executor.execute(sendMessageTask);
    }

    private boolean processCommand(CommandType commandType, Object[] responses) {
        boolean isDebug = log.isDebugEnabled();
        String subscriptionChannel = null;
        int channelCount = -1;
        if(isDebug) {
            log.debug("BEGIN PROCESS COMMAND");
        }
        StringBuilder sb = null;
        if(responses != null) {
            subscriptionChannel = (String) responses[1];
            channelCount = (Integer) responses[2];
            if(isDebug) {
                if(sb == null) {
                    sb = new StringBuilder();
                }
                sb.setLength(0);
                sb.append("Processing responses:");
                for(int i = 0; i < responses.length; i++) {
                    sb.append(' ').append(responses[i]);
                }
                log.debug(sb.toString());
            }
        }
        else {
            if(isDebug) {
                log.debug("Processing null responses");
            }
        }
        boolean isNoResponseCommand = false;
        RedisCommandObject commandObject = null;
        Object[] params = null;
        RedisCommand command = null;
        do {
            isNoResponseCommand = false;
            commandObject = responseQueue.remove(0, false);
            if(commandObject != null) {
                RedisCommandInfo commandInfo = commandObject.getCommandInfo();
                params = commandInfo.getParams();
                command = commandInfo.getCommand();
                if(isDebug) {
                    if(sb == null) {
                        sb = new StringBuilder();
                    }
                    sb.setLength(0);
                    sb.append("Dequed Next Command: ").append(command);
                    for(int i = 0; i < params.length; i++) {
                        sb.append(' ').append(params[i]);
                    }
                    log.debug(sb.toString());
                }
                if(params.length == 0) {
                    if(command == RedisCommand.UNSUBSCRIBE && subscriptionMap.size() == 0) {
                        isNoResponseCommand = true;
                    }
                    if(command == RedisCommand.PUNSUBSCRIBE && patternSubscriptionMap.size() == 0) {
                        isNoResponseCommand = true;
                    }
                    if(commandType != null && subscriptionChannel == null) {
                        // redis 2.6+ send a response with null channel for redundant unsubscribeAll
                        isNoResponseCommand = false;
                    }
                }
            }
        } while(isNoResponseCommand);
        boolean persistConnection = false;
        if(commandType != null && subscriptionChannel == null) {
            // Ignore empty channel unsubscribe response of redis 2.6+
        }
        else if(commandObject != null) {
            SubscriptionInfo subscriptionInfo = commandObject.subscriptionInfo;
            if(commandType == null) {
                commandType = CommandType.valueOf(command.name());
            }
            if(subscriptionChannel == null) {
                subscriptionChannel = "__AREDIS_DUMMY1__";
                if(params.length > 0) {
                    subscriptionChannel = (String) params[0];
                }
                channelCount = 0;
            }
            switch(commandType) {
              case SUBSCRIBE:
                if(command == RedisCommand.SUBSCRIBE && subscriptionChannel.equals(params[0])) {
                    if(subscriptionInfo != null) {
                        subscriptionMap.put(subscriptionChannel, subscriptionInfo);
                    }
                }
                else {
                    log.error("Internal Error: SUBSCRIBE " + subscriptionChannel + " COMMAND EXPECTED BUT FOUND: " + command + " " + (params.length > 0 ? " " + params[0] : ""));
                }
                if(channelCount == 1) {
                    persistConnection = true;
                }
                break;
              case PSUBSCRIBE:
                if(command == RedisCommand.PSUBSCRIBE && subscriptionChannel.equals(params[0])) {
                    if(subscriptionInfo != null) {
                        patternSubscriptionMap.put(subscriptionChannel, subscriptionInfo);
                    }
                }
                else {
                    log.error("Internal Error: PSUBSCRIBE " + subscriptionChannel + " COMMAND EXPECTED BUT FOUND: " + command + " " + (params.length > 0 ? " " + params[0] : ""));
                }
                if(channelCount == 1) {
                    persistConnection = true;
                }
                break;
              case UNSUBSCRIBE:
                if(command == RedisCommand.UNSUBSCRIBE && (params.length == 0 || subscriptionChannel.equals(params[0]))) {
                    subscriptionMap.remove(subscriptionChannel);
                    if(params.length == 0 && subscriptionMap.size() > 0) {
                        responseQueue.push(commandObject);
                    }
                }
                else {
                    log.error("Internal Error: UNSUBSCRIBE [" + subscriptionChannel + "] COMMAND EXPECTED BUT FOUND: " + command + (params.length > 0 ? " " + params[0] : ""));
                }
                break;
              case PUNSUBSCRIBE:
                if(command == RedisCommand.PUNSUBSCRIBE && (params.length == 0 || subscriptionChannel.equals(params[0]))) {
                    patternSubscriptionMap.remove(subscriptionChannel);
                    if(params.length == 0 && patternSubscriptionMap.size() > 0) {
                        responseQueue.push(commandObject);
                    }
                }
                else if(responses != null) {
                    log.error("Internal Error: PUNSUBSCRIBE [" + subscriptionChannel + "] COMMAND EXPECTED BUT FOUND: " + command + (params.length > 0 ? " " + params[0] : ""));
                }
                break;
              default:
                break;
            }
        }
        else {
            log.error("Internal error: COMMAND not found in Response Q for " + commandType + " command");
        }
        boolean continueListening = subscriptionMap.size() > 0 || patternSubscriptionMap.size() > 0;
        if(!isResponseErrored) {
            if(!continueListening) {
                AsyncSocketTransportConfig config = con.getConfig();
                config.setReadTimeoutMillis(savedConfig.getReadTimeoutMillis());
                config.setMaxIdleTimeMillis(savedConfig.getMaxIdleTimeMillis());
            }
            else if(persistConnection) {
                AsyncSocketTransportConfig config = con.getConfig();
                config.setReadTimeoutMillis(0);
                config.setMaxIdleTimeMillis(Integer.MAX_VALUE);
            }
        }
        if(isDebug) {
            log.debug("END PROCESS COMMAND " + continueListening);
        }
        return continueListening;
    }

    private class ResponseHandler implements AsyncHandler<RedisRawResponse> {
        public boolean processRedisResponse(RedisRawResponse response) {
            boolean processNextResponse = false;
            do {
                Object responses[] = extractResponseInfo((RedisRawResponse[]) response.getResult());
                CommandType commandType = (CommandType) responses[0];
                if(commandType == CommandType.MESSAGE || commandType == CommandType.PMESSAGE) {
                    processMessage(commandType, responses);
                }
                else {
                    processNextResponse = !processCommand(commandType, responses);
                }
                response = null;
                if(!processNextResponse) {
                    response = responseReader.readResponse(responseHandler);
                }
            }
            while(response != null);

            return processNextResponse;
        }

        @Override
        public void completed(RedisRawResponse result, Throwable e) {
            // System.out.println("ENTERED RESPONSE HANDLER e = " + e);
            boolean processNextResponse = true;
            if(result == null || e != null) {
                isResponseErrored = true;
                e.printStackTrace();
            }
            else {
                processNextResponse = processRedisResponse(result);
            }
            if(processNextResponse) {
                processNextResponses();
            }
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

    private class StartRequestProcessingTask extends Thread {
        @Override
        public void run() {
            processNextRequests();
        }
    }

    private class StartResponseProcessingTask extends Thread {
        @Override
        public void run() {
            processNextResponses();
        }
    }

    private class ErrorRetryTask implements Runnable {
        private volatile boolean isActive;

        @Override
        public void run() {
            isActive = false;
            RedisCommandInfo commandInfo = new RedisCommandInfo(RedisCommand.UNSUBSCRIBE, "__AREDIS_DUMMY__");
            submitCommand(commandInfo, null);
        }
    }

    private static final Log log = LogFactory.getLog(RedisSubscriptionConnection.class);

    static {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10, 10, 15, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        threadPoolExecutor.allowCoreThreadTimeOut(true);
    }

    int flushCount;

    private AsyncSocketTransport con;

    private AsyncSocketTransportConfig savedConfig;

    SingleConsumerQueue<RedisCommandObject> requestQueue;

    SingleConsumerQueue<RedisCommandObject> responseQueue;

    /**
     * Executor used for calling back message listeners.
     */
    protected Executor taskExecutor;

    private int dbIndex;

    private FlushHandler flushHandler;

    private ConnectHandler connectHandler;

    private RequestHandler requestHandler;

    private StartRequestProcessingTask startRequestProcessingTask;

    private StartResponseProcessingTask startResponseProcessingTask;

    private ResponseHandler responseHandler;

    private RedisResponseReader responseReader;

    private boolean isResponseErrored;

    private volatile boolean isShutDown;

    private Map<String, SubscriptionInfo> subscriptionMap;

    private Map<String, SubscriptionInfo> patternSubscriptionMap;

    private ErrorRetryTask errorRetryTask;

    /**
     * Creates a RedisSubscriptionConnection.
     * @param pcon Async Socket Transport for the connection
     * @param pdbIndex dbIndex of the connection
     * @param ptaskExecutor Executor to use for sending the messages to the listeners. It is strongly recommended to pass a non null value.
     * If null an internal Thread Pool will be used to make the calls to the listeners.
     */
    public RedisSubscriptionConnection(AsyncSocketTransport pcon, int pdbIndex, Executor ptaskExecutor) {
        con = pcon;
        AsyncSocketTransportConfig config = con.getConfig();
        savedConfig = config;
        con.setConfig(config.clone());
        dbIndex = pdbIndex;
        taskExecutor = ptaskExecutor;
        requestQueue = new SingleConsumerQueue<RedisCommandObject>();
        RequestQueueIdleListener idleListener = new RequestQueueIdleListener();
        requestQueue.setIdleListener(idleListener);
        responseQueue = new SingleConsumerQueue<RedisCommandObject>();
        connectHandler = new ConnectHandler();
        requestHandler = new RequestHandler();
        responseHandler = new ResponseHandler();
        responseReader = new RedisResponseReader(con);;
        flushHandler = new FlushHandler();
        startResponseProcessingTask = new StartResponseProcessingTask();
        startRequestProcessingTask = new StartRequestProcessingTask();
        subscriptionMap = new ConcurrentHashMap<String, SubscriptionInfo>();
        patternSubscriptionMap = new ConcurrentHashMap<String, SubscriptionInfo>();
    }

    private void resetOnConnect() {
        isResponseErrored = false;
    }

    private void moveToResponseQueue(RedisCommandObject processedCommandObject) {
        if(processedCommandObject != null) {
            boolean startResponseProcessing = responseQueue.add(processedCommandObject, true);
            if(startResponseProcessing) {
                AsyncRedisConnection.bootstrapExecutor.execute(startResponseProcessingTask);
            }
        }
    }

    private boolean processNextRequest() {
        boolean isSyncSend = false;
        RedisCommandObject commandObject;
        ConnectionStatus connectionStatus = con.getStatus();
        StringBuilder sb = null;
        boolean isDebug = log.isDebugEnabled();
        if(isDebug) {
            log.debug("Processing Next Request");
        }
        if(connectionStatus == ConnectionStatus.CLOSED ||
           connectionStatus == ConnectionStatus.STALE ||
           connectionStatus == ConnectionStatus.RETRY) {
            if(requestQueue.size() == 0 && responseQueue.size() == 0 && con.isStale()) {
                try {
                    con.close();
                } catch (IOException e) {
                    log.error("Error closing idle connection " + con, e);
                }
                requestQueue.markIdle();
                if(requestQueue.size() > 0 && requestQueue.acquireIdle()) {
                    isSyncSend = processNextRequest();
                }
            }
            else {
                if(isDebug) {
                    log.debug("ProcessNextRequest> Checking response Q size " + responseQueue.size());
                }
                boolean idle = false;
                if(responseQueue.size() > 0) {
                    requestQueue.markIdle();
                    if(responseQueue.size() == 0) {
                        idle = !requestQueue.acquireIdle();
                    }
                }
                if(isDebug) {
                    log.debug("Got Try to connect = " + (!idle));
                }
                if(!idle) {
                    con.connect(connectHandler);
                }
            }
        }
        else if(connectionStatus == ConnectionStatus.OK) {
            long timeoutMicros = 100;
            boolean flushPending = false;
            if(con.requiresFlush()) {
                flushPending = true;
            }
            commandObject = requestQueue.remove(timeoutMicros, !flushPending);
            if(commandObject != null) {
                moveToResponseQueue(commandObject);
                if(isDebug) {
                    if(sb == null) {
                        sb = new StringBuilder();
                    }
                    sb.setLength(0);
                    sb.append("Queing ").append(commandObject.commandInfo.getCommand());
                    for(int i = 0; i < commandObject.commandInfo.getParams().length; i++) sb.append(' ').append(commandObject.commandInfo.getParams()[i]);
                    log.debug(sb.toString());
                }
                isSyncSend = commandObject.sendRequest(con, requestHandler) > 0;
                // System.out.println("SYNCSEND = " + isSyncSend);
            }
            else {
                if(flushPending) {
                    con.flush(flushHandler);
                }
            }
        }
        else {
            // connectionStatus == ConnectionStatus.DOWN
            // Send immediate skipped response
            commandObject = requestQueue.remove(0, true);
            if(isDebug) {
                log.debug("ProcessNextRequest> For Connection Status DOWN got commandObject " + commandObject);
            }
            if(commandObject != null) {
                isSyncSend = true;
                moveToResponseQueue(commandObject);
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

    private void processNextResponses() {
        boolean isSyncReceive = false;
        long timeoutMicros = 1000;
        boolean isDebug = log.isDebugEnabled();
        if(isResponseErrored) {
            if(isDebug) {
                log.debug("RESPONSE ERRORED");
            }
            ConnectionStatus connectionStatus = con.getStatus();
            if(connectionStatus == ConnectionStatus.DOWN || connectionStatus == ConnectionStatus.RETRY) {
                timeoutMicros = 0;
                if(errorRetryTask == null) {
                    errorRetryTask = new ErrorRetryTask();
                }
                if(!errorRetryTask.isActive) {
                    long downSince = con.getDownSince();
                    long retryMillis = 0;
                    if(downSince > 0) {
                        retryMillis = con.getRetryInterval() + downSince - System.currentTimeMillis();
                    }
                    if(isDebug) {
                        log.debug("STARTING RETRY original retryMillis: " + con.getRetryInterval() + " calculated " + retryMillis);
                    }
                    if(retryMillis <= 0) {
                        retryMillis = 100;
                    }
                    else if(retryMillis < 10){
                        retryMillis = 10;
                    }
                    errorRetryTask.isActive = true;
                    AsyncRedisFactory.getTimer().schedule(errorRetryTask, retryMillis);
                }
            }
        }
        do {
            isSyncReceive = false;
            RedisCommandObject commandObject = responseQueue.remove(timeoutMicros, true);
            if(isDebug) {
                log.debug("GOT RESPONSE DEQUEUE " + commandObject);
            }
            if(commandObject == null) {
                // System.out.println("Request Q size " + requestQueue.size());
                if(requestQueue.size() > 0) {
                    // System.out.println("Trying to acquire request Q");
                    if(requestQueue.acquireIdle()) {
                        // System.out.println("acquired request Q");
                        AsyncRedisConnection.bootstrapExecutor.execute(startRequestProcessingTask);
                    }
                }
                else {
                    // The below acquire and release is done to close the connection if shutting down
                    // con should not be closed outside idle listener as a new request may
                    // arrive concurrently before sutdown delay
                    if(isShutDown && requestQueue.acquireIdle()) {
                        requestQueue.markIdle();
                    }
                }
                break;
            }
            responseQueue.push(commandObject);
            if(isResponseErrored) {
                isSyncReceive = true;
                // Do a dummy processCommand to update subscriptionMaps
                processCommand(null, null);
            }
            else {
                RedisRawResponse response = responseReader.readResponse(responseHandler);
                if(response != null) {
                   isSyncReceive = responseHandler.processRedisResponse(response);
                }
            }
        } while(isSyncReceive);
    }

    /**
     * Submit a Redis Subscribe or Unsubscribe Command.
     * @param commandInfo CommandInfo containing the command which has to be SUBSCRIBE, UNSUSCRIBE, PSUBSCRIBE or PUNSUBSCRIBE
     * @param subscriptionInfo subscription info
     */
    protected void submitCommand(RedisCommandInfo commandInfo, SubscriptionInfo subscriptionInfo) {
        if(isShutDown) {
            throw new IllegalStateException("" + this.toString() + " Cannot accepot commands after shutdown time.");
        }
        boolean processRequest = false;
        RedisCommandObject commandObject = null;
        try {
            commandObject = new RedisCommandObject(commandInfo, null);
            commandObject.subscriptionInfo = subscriptionInfo;
            commandObject.commandInfo.serverInfo = con;
            commandObject.generateRequestData(con);
            processRequest = true;
        } catch (IOException e) {
            processRequest = false;
            log.error("Error Generating Request Data", e);
        }
        if(processRequest) {
            boolean startRequestProcessing = requestQueue.add(commandObject, true);
            if(startRequestProcessing) {
                new Thread(startRequestProcessingTask).start();
            }
        }
    }

    /**
     * Closes the connection if it has been idle beyond the configured idle timeout or the Redis Server timeout.
     * The connection is not closed if there are any active subscriptions. A connection will be automatically re-established on the next command.
     */
    public void closeStaleConnection() {
        if(con.isStale() && requestQueue.acquireIdle()) {
            AsyncRedisConnection.bootstrapExecutor.execute(startRequestProcessingTask);
        }
    }

    /**
     * Shutdown the connection.
     * This method however is currently not the preferred way to shutdown because the socket
     * connections close automatically once the JVM is eligible for shutdown.
     */
    public void shutdown() {
        if(isShutDown) {
            log.warn(this.toString() + ": Ignoring duplicate shutdown call");
        }
        else {
            isShutDown = true;
            // Acquuire and release request Q if empty so that connection is closed
            if(requestQueue.acquireIdle()) {
                requestQueue.markIdle();
            }
        }
    }

    /**
     * Returns the Async Socket Transport used
     * @return Async Socket Transport used
     */
    public AsyncSocketTransport getConnection() {
        return con;
    }

}
