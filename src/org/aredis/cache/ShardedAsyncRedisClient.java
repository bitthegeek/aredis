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
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.cache.RedisCommandInfo.CommandStatus;
import org.aredis.cache.RedisCommandInfo.ResultType;
import org.aredis.net.ConnectionStatus;
import org.aredis.util.ArrayWrappingList;
import org.aredis.util.concurrent.EmptyRunnable;

/**
 * ShardedAsyncRedisClient is an implementation of AsynRedisClient which holds 2 or more {@link AsyncRedisConnection}
 * Objects and forwards the commands to one of them based on the hash of the key in the command. If there are multiple keys all the keys should map
 * to the same server unless aredis supports splitting of the command into multiple commands and aggregating the results of those commands.
 * MGET, MSET and DEL are the commands with multiple keys for which splitting of the command is supported. If multiple keys map to multiple servers
 * and the command is not one of these then an IllegalArgumentException is thrown.
 * @author Suresh
 *
 */
public class ShardedAsyncRedisClient extends AbstractAsyncRedisClient {

    private static class ResponseHandler implements AsyncHandler<RedisCommandInfo> {

        private class FinalResponseSender implements Runnable {

            @Override
            public void run() {
                if(finalResponseHandler != null) {
                    finalResponseHandler.completed(shardedCommand, null);
                }
            }

        }

        private RedisCommandInfo shardedCommand;

        private AtomicInteger pendingCount;

        private boolean isSyncCallback;

        private FutureTask<RedisCommandInfo> futureResult;

        private AsyncHandler<RedisCommandInfo> finalResponseHandler;

        private Executor executor;

        public ResponseHandler(RedisCommandInfo pshardedCommand, AtomicInteger ppendingCount, boolean pisSyncCallback, boolean prequireFutureResult, AsyncHandler<RedisCommandInfo> pfinalResponseHandler, Executor pexecutor) {
            shardedCommand = pshardedCommand;
            pendingCount = ppendingCount;
            isSyncCallback = pisSyncCallback;
            if(prequireFutureResult) {
                futureResult = new FutureTask<RedisCommandInfo>(EmptyRunnable.instance, shardedCommand);
            }
            finalResponseHandler = pfinalResponseHandler;
            executor = pexecutor;
        }

        @Override
        public void completed(RedisCommandInfo result, Throwable e) {
            int i, len, remain = pendingCount.decrementAndGet();
            if(remain <= 0) {
                RedisCommandInfo[] splitCommands = shardedCommand.splitCommands;
                len = 0;
                if(splitCommands != null) {
                    len = splitCommands.length;
                }
                CommandStatus runStatus = null;
                ResultType resultType = null;
                for(i = 0; i < len; i++) {
                    CommandStatus nextRunStatus = splitCommands[i].runStatus;
                    ResultType nextResultType = splitCommands[i].resultType;
                    if(resultType == null || nextResultType != ResultType.REDIS_ERROR) {
                        resultType = nextResultType;
                    }
                    if(nextRunStatus == CommandStatus.SUCCESS) {
                        runStatus = nextRunStatus;
                        break;
                    }
                    if(nextRunStatus == CommandStatus.NETWORK_ERROR) {
                        runStatus = nextRunStatus;
                    }
                    else if(runStatus != CommandStatus.NETWORK_ERROR) {
                        runStatus = nextRunStatus;
                    }
                }
                shardedCommand.resultType = result.resultType;
                shardedCommand.runStatus = runStatus;
                sendFinalResponse();
            }
        }

        public void sendFinalResponse() {
            if(futureResult != null) {
                futureResult.run();
            }
            if(finalResponseHandler != null) {
                FinalResponseSender finalResponseSender = new FinalResponseSender();
                if(!isSyncCallback) {
                    if(executor == null) {
                        executor = AsyncRedisConnection.bootstrapExecutor;
                    }
                    executor.execute(finalResponseSender);
                }
                else {
                    try {
                        finalResponseSender.run();
                    }
                    catch(Exception e) {
                        log.error("Ignoring Error During Sync Callback", e);
                    }
                }
            }
        }
    }

    private AsyncRedisConnection connections[];

    private List<AsyncRedisConnection> connectionList;

    private KeyHasher keyHasher;

    private Executor executor;

    private static final Log log = LogFactory.getLog(ShardedAsyncRedisClient.class);

    /**
     * It is preferable to use {@link AsyncRedisFactory} than this constructor.
     * @param pconnections Underlying AsyncRedisConnections
     * @param pexecutor Task Executor to use for Asyc APIs. Can be null if you do not use Async APIs with completion handler
     * @param pkeyHasher KeyHasher to use. ConsistentKeyHasher is used as default if null is passed.
     */
    public ShardedAsyncRedisClient(List<AsyncRedisConnection> pconnections, Executor pexecutor, KeyHasher pkeyHasher) {
        if(pconnections.size() == 0) {
            throw new IllegalArgumentException("pconnections.size() == 0");
        }
        keyHasher = pkeyHasher;
        executor = pexecutor;
        connections = pconnections.toArray(new AsyncRedisConnection[pconnections.size()]);
        connectionList = new ArrayWrappingList<AsyncRedisConnection>(connections);
        if(keyHasher == null) {
            keyHasher = new ConsistentKeyHasher(connections);
        }
    }

    /**
     * Constructor which uses ConsistentKeyHasher as default.
     * @param pconnections Underlying AsyncRedisConnections
     * @param pexecutor Task Executor to use for Asyc APIs. Can be null if you do not use Async APIs with completion handler
     */
    public ShardedAsyncRedisClient(List<AsyncRedisConnection> pconnections, Executor pexecutor) {
        this(pconnections, pexecutor, null);
    }

    /**
     * Basic Constructor.
     * @param pconnections Underlying AsyncRedisConnections
     */
    public ShardedAsyncRedisClient(List<AsyncRedisConnection> pconnections) {
        this(pconnections, null, null);
    }

    /**
     * Untility Method for use by KeyHashers to return the useable connections.
     * @param connections Array of connections
     * @param useable A boolean array of same size containing true for those connections which are useable
     * @return A status value to quickly identify the contents of useable array. A return value of -connections.length
     * indicates that all connections are useable which is the ideal thing. A value >= 0 indicates that only 1 connection
     * whose index is the value returned is useable. Any other negative value is -(num flags). num flags will not be 0
     * because if all flags are false all flags are set to true so that the command is sent anyway.
     */
    public static int getUseableConnections(AsyncRedisConnection[] connections, boolean useable[]) {
        int lastUseableIndex = -1, flagCount = 0; // >=0 Only 1 flag at returned index is set else -(num flags) is returned
        int i, statusToUseOrdinal = -1;

        for(i = 0; i < connections.length; i++) {
            ConnectionStatus status = connections[i].getConnection().getStatus();
            if(status == ConnectionStatus.DOWN || status == ConnectionStatus.RETRY) {
                if(status == ConnectionStatus.RETRY) {
                    connections[i].submitCommand(new RedisCommandInfo(RedisCommand.PING), null);
                }
            }
            else {
                status = ConnectionStatus.OK;
            }
            int statusOrdinal = status.ordinal();
            if(statusToUseOrdinal < 0) {
                statusToUseOrdinal = statusOrdinal;
            }
            if(lastUseableIndex == -1) {
                if(statusOrdinal != statusToUseOrdinal) {
                    if(statusOrdinal < statusToUseOrdinal) {
                        lastUseableIndex = i;
                        statusToUseOrdinal = statusOrdinal;
                        useable[i] = true;
                        flagCount = 1;
                    }
                    else {
                        lastUseableIndex = i - 1;
                        flagCount = i;
                        Arrays.fill(useable, 0, i, true);
                    }
                }
            }
            else if(statusOrdinal <= statusToUseOrdinal) {
                if(statusOrdinal < statusToUseOrdinal) {
                    flagCount = 0;
                    statusToUseOrdinal = statusOrdinal;
                    Arrays.fill(useable, 0, i, false);
                }
                flagCount++;
                lastUseableIndex = i;
                useable[i] = true;
            }
        }
        if(lastUseableIndex == -1) {
            // If all connections are of same status mark all of them as useable since any
            // of them can be picked
            Arrays.fill(useable, true);
            lastUseableIndex = connections.length - 1;
            flagCount = connections.length;
        }
        int statusCode = lastUseableIndex;
        if(flagCount > 1) {
            statusCode = -flagCount;
        }

        return statusCode;
    }

    @Override
    public Future<RedisCommandInfo[]> submitCommands(
            RedisCommandInfo[] commandInfos,
            AsyncHandler<RedisCommandInfo[]> completionHandler,
            boolean requireFutureResult, boolean isSyncCallback) {
        if(connections.length > 1) {
            throw new UnsupportedOperationException("Submit Commands not allowed on a Sharded Client with multiple connections");
        }
        return connections[0].submitCommands(commandInfos, completionHandler, requireFutureResult, isSyncCallback);
    }

    @Override
    public Future<RedisCommandInfo> submitCommand(RedisCommandInfo commandInfo,
            AsyncHandler<RedisCommandInfo> completionHandler,
            boolean requireFutureResult, boolean isSyncCallback) {
        Future<RedisCommandInfo> futureResult = null;
        Object[] params = commandInfo.getParams();
        RedisCommand command = commandInfo.getCommand();
        int numKeys = 0;
        int i, uniqueConnectionIndex = -1; // -1: 0 Keys, -2: Requires sharding, >=0: single connection Index
        if(connections.length > 1) {
            char[] argTypes = command.argTypes;
            int repeatableFromIndex = command.getRepeatableFromIndex();
            int argTypeIndex = -1;
            int firstKeyIndex = -1;
            // Determine the index of the 1st Key
            for(i = 0; i < params.length; i++) {
                // Not considering c for count of args since currently there are no shardeable commands with c
                argTypeIndex++;
                if(argTypeIndex >= argTypes.length) {
                    argTypeIndex = repeatableFromIndex;
                    if(repeatableFromIndex < 0) {
                        throw new IllegalArgumentException("Extra parameter '" + params[i] + "' at position " + (i + 1) + " for command " + command + ". Only " + argTypes.length + " parameter(s) expected.");
                    }
                }
                char argType = argTypes[argTypeIndex];
                if(argType == 'k') {
                    if(firstKeyIndex < 0) {
                        firstKeyIndex = i;
                    }
                    numKeys++;
                }
            }
            if(numKeys == 0) {
                throw new IllegalArgumentException("Command to a Sharded Connection " + command.name() + " submitted without keys");
            }
            int shardCount = 0;
            RedisCommandInfo splitCommandsByIndex[] = null;
            if(numKeys > 1) {
                int connectionIndexes[] = new int[params.length];
                Arrays.fill(connectionIndexes, -1);
                // When splitting we assume all params till the first key (Currently 0 for all
                // commands) is common to all sharded commands and the non key params if any after each key
                // goes to the connection member for the key like MSET
                int paramCounts[] = new int[connections.length];
                Arrays.fill(paramCounts, firstKeyIndex);
                int prevKeyIndex = 0;
                argTypeIndex = -1;
                int prevConnectionIndex = -1;
                for(i = 0; i < params.length; i++) {
                    // Not considering c for count of args since currently there are no shardeable commands with c
                    argTypeIndex++;
                    if(argTypeIndex >= argTypes.length) {
                        argTypeIndex = repeatableFromIndex;
                    }
                    char argType = argTypes[argTypeIndex];
                    if(argType == 'k') {
                        int connectionIndex = keyHasher.getConnectionIndex((String) params[i], connections, true);
                        if(connectionIndex < 0) {
                            uniqueConnectionIndex = -3;
                            break;
                        }
                        connectionIndexes[i] = connectionIndex;
                        if(uniqueConnectionIndex == -1) {
                            uniqueConnectionIndex = connectionIndex;
                        }
                        else if(uniqueConnectionIndex != connectionIndex){
                            uniqueConnectionIndex = -2;
                        }
                        if(prevConnectionIndex >= 0) {
                            paramCounts[prevConnectionIndex] += (i - prevKeyIndex);
                        }
                        prevKeyIndex = i;
                        prevConnectionIndex = connectionIndex;
                    }
                }
                if(uniqueConnectionIndex == -2) {
                    if(prevConnectionIndex >= 0) {
                        paramCounts[prevConnectionIndex] += (i - prevKeyIndex);
                    }
                    for(i = 0; i < paramCounts.length; i++) {
                        if(paramCounts[i] > firstKeyIndex) {
                            shardCount++;
                        }
                    }
                    command = commandInfo.getCommand();
                    RedisCommandInfo splitCommands[] = new RedisCommandInfo[shardCount];
                    splitCommandsByIndex = new RedisCommandInfo[connections.length];
                    shardCount = 0;
                    for(i = 0; i < paramCounts.length; i++) {
                        if(paramCounts[i] > firstKeyIndex) {
                            Object nextParams[] = new Object[paramCounts[i]];
                            if(firstKeyIndex > 0) {
                                System.arraycopy(params, 0, nextParams, 0, firstKeyIndex);
                            }
                            RedisCommandInfo nextCommandInfo = new RedisCommandInfo(command, nextParams);
                            nextCommandInfo.connectionIndex = i;
                            splitCommands[shardCount] = nextCommandInfo;
                            splitCommandsByIndex[i] = nextCommandInfo;
                            paramCounts[i] = 0;
                            shardCount++;
                        }
                    }
                    int connectionIndex = -1;
                    for(i = firstKeyIndex; i < params.length; i++) {
                        int nextConnectionIndex = connectionIndexes[i];
                        if(nextConnectionIndex >= 0) {
                            connectionIndex = nextConnectionIndex;
                        }
                        Object nextParams[] = splitCommandsByIndex[connectionIndex].getParams();
                        nextParams[paramCounts[connectionIndex]++] = params[i];
                    }
                    commandInfo.splitCommands = splitCommands;
                }
            }
            else {
                String key = (String) params[firstKeyIndex];
                uniqueConnectionIndex = keyHasher.getConnectionIndex(key, connections, true);
                if(uniqueConnectionIndex < 0) {
                    uniqueConnectionIndex = -3;
                }
            }
            if(uniqueConnectionIndex == -2) {
                if(command.shardedResultHandler != null) {
                    ResponseHandler responseHandler = new ResponseHandler(commandInfo, new AtomicInteger(shardCount), isSyncCallback, requireFutureResult, completionHandler, executor);
                    for(i = 0; i < splitCommandsByIndex.length; i++) {
                        RedisCommandInfo nextCommandInfo = splitCommandsByIndex[i];
                        if(nextCommandInfo != null) {
                            connections[i].submitCommand(nextCommandInfo, responseHandler, false, true);
                        }
                    }
                    futureResult = responseHandler.futureResult;
                }
                else {
                    throw new IllegalArgumentException("Command " + command + " with multiple keys mapping to different servers is not shardeable.");
                }
            }
            else if(uniqueConnectionIndex >= 0) {
                futureResult = connections[uniqueConnectionIndex].submitCommand(commandInfo, completionHandler, requireFutureResult, false);
            }
            else if(uniqueConnectionIndex == -1){
                throw new IllegalArgumentException("Cannot Execute command " + command + " on a ShardedAsyncRedisClient since it has no keys");
            }
            else {
                commandInfo.runStatus = CommandStatus.SKIPPED;
                ResponseHandler responseHandler = new ResponseHandler(commandInfo, null, isSyncCallback, requireFutureResult, completionHandler, executor);
                futureResult = responseHandler.futureResult;
                responseHandler.sendFinalResponse();
            }
        }
        else {
            futureResult = connections[0].submitCommand(commandInfo, completionHandler, requireFutureResult, false);
        }
        return futureResult;
    }

    /**
     * Gets List of connections
     * @return A List containing the underlying connections
     */
    public List<AsyncRedisConnection> getConnections() {
        return connectionList;
    }

    /**
     * Gets the Keyhasher being used
     * @return KeyHasher used for distributing a key amongst connections
     */
    public KeyHasher getKeyHasher() {
        return keyHasher;
    }

    /**
     * Sets the Keyhasher
     * @param pkeyHasher KeyHasher to use for distributing a key amongst connections
     */
    public void setKeyHasher(KeyHasher pkeyHasher) {
        keyHasher = pkeyHasher;
    }

}
