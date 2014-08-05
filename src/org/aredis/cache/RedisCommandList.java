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
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.cache.RedisCommandInfo.CommandStatus;
import org.aredis.cache.RedisCommandInfo.ResultType;
import org.aredis.net.AsyncSocketTransport;
import org.aredis.net.ServerInfo;
import org.aredis.util.concurrent.EmptyRunnable;

class RedisCommandList implements AsyncHandler<Integer> {

    private class IndividualResponseHandler implements AsyncHandler<RedisCommandInfo> {

        private AsyncHandler<RedisCommandList> responseHandler;

        private int responseIndex;

        private RedisCommandList parent;

        public IndividualResponseHandler(RedisCommandList pparent) {
            parent = pparent;
        }

        @Override
        public void completed(RedisCommandInfo result, Throwable e) {
            if(result != null && result.runStatus == CommandStatus.SUCCESS && e == null) {
                int index;
                while((index = ++responseIndex) < endIndex && commandObjects[index].receiveResponse(con, this) != null);
                if(index >= endIndex) {
                    responseHandler.completed(parent, null);
                }
            }
            else {
                responseHandler.completed(null, e);
            }
        }

    }

    private class FinalResponseSender implements Runnable {

        @Override
        public void run() {
            try {
                AsyncRedisConnection.insideCallback.set(true);
                aredis.updatePipelineSizeForResponse(RedisCommandList.this);
                if(finalResponsesHandler != null) {
                    finalResponsesHandler.completed(commandInfos, null);
                }
                else if(finalResponseHandler != null) {
                    finalResponseHandler.completed(commandInfos[0], null);
                }
            }
            finally {
                AsyncRedisConnection.insideCallback.remove();
            }
        }

    }

    private static final Log log = LogFactory.getLog(RedisCommandList.class);

    private AsyncHandler<RedisCommandInfo[]> finalResponsesHandler;

    private AsyncHandler<RedisCommandInfo> finalResponseHandler;

    RunnableFuture<RedisCommandInfo[]> futureResults;

    RunnableFuture<RedisCommandInfo> futureResult;

    boolean isSyncCallback;

    private RedisCommandObject commandObjects[];

    private AsyncHandler<RedisCommandList> requestHandler;

    private int requestIndex;

    private AsyncSocketTransport con;

    private ScriptStatuses scriptStatuses;

    RedisCommandInfo commandInfos[];

    private IndividualResponseHandler individualResponseHandler;

    int numPipelinedCammands;

    int finalDbIndex;

    private int startIndex;

    int endIndex;

    boolean hasSelectCommands;

    private boolean hasScriptCommands;

    private AsyncRedisConnection aredis;

    public RedisCommandList(RedisCommandInfo pcommandInfos[], Queue<RedisCommandInfo> pqueuedMultiCommands, AsyncHandler<RedisCommandInfo[]> pfinalResponseHandler, boolean requireFutureResults) {
        commandInfos = pcommandInfos;
        commandObjects = new RedisCommandObject[pcommandInfos.length];
        startIndex = 0;
        endIndex = commandInfos.length;
        int i;
        for(i = 0; i < commandObjects.length; i++) {
            RedisCommandInfo commandInfo = pcommandInfos[i];
            commandInfo.runStatus = CommandStatus.SKIPPED;
            commandObjects[i] = new RedisCommandObject(commandInfo , pqueuedMultiCommands);
        }
        individualResponseHandler = new IndividualResponseHandler(this);
        finalResponsesHandler = pfinalResponseHandler;
        if(requireFutureResults) {
            futureResults = new FutureTask<RedisCommandInfo[]>(EmptyRunnable.instance, commandInfos);
        }
    }

    public RedisCommandList(RedisCommandInfo pcommandInfo, Queue<RedisCommandInfo> pqueuedMultiCommands, AsyncHandler<RedisCommandInfo> pfinalResponseHandler, boolean requireFutureResult) {
        commandInfos = new RedisCommandInfo[1];
        commandInfos[0] = pcommandInfo;
        startIndex = 0;
        endIndex = commandInfos.length;
        commandObjects = new RedisCommandObject[1];
        pcommandInfo.runStatus = CommandStatus.SKIPPED;
        commandObjects[0] = new RedisCommandObject(pcommandInfo , pqueuedMultiCommands);
        individualResponseHandler = new IndividualResponseHandler(this);
        finalResponseHandler = pfinalResponseHandler;
        if(requireFutureResult) {
            futureResult = new FutureTask<RedisCommandInfo>(EmptyRunnable.instance, pcommandInfo);
        }
    }

    public void generateRequestData(ServerInfo serverInfo) throws IOException {
        int i;
        for(i = 0; i < commandObjects.length; i++) {
            commandObjects[i].generateRequestData(serverInfo);
        }
    }

    @Override
    public void completed(Integer result, Throwable e) {
        if(result >= 0 && e == null) {
            do {
                commandObjects[requestIndex].requestData = null;
                int index = ++requestIndex;
                if(index < endIndex) {
                    commandInfos[index].runStatus = CommandStatus.NETWORK_ERROR;
                    if(commandObjects[index].sendRequest(con, this, scriptStatuses) <= 0) {
                        break;
                    }
                }
                else {
                    requestHandler.completed(this, null);
                    break;
                }
            } while(true);
        }
        else {
            requestHandler.completed(null, e);
        }
    }

    public boolean sendRequest(AsyncSocketTransport pcon, AsyncHandler<RedisCommandList> prequestHandler, ScriptStatuses scriptStats) {
        requestHandler = prequestHandler;
        requestIndex = 0;
        con = pcon;
        scriptStatuses = scriptStats;
        int len = 0;
        for(requestIndex = startIndex; requestIndex < endIndex; requestIndex++) {
            commandInfos[requestIndex].runStatus = CommandStatus.NETWORK_ERROR;
            len = commandObjects[requestIndex].sendRequest(con, this, scriptStats);
            if(len > 0) {
                commandObjects[requestIndex].requestData = null;
            }
            else {
                break;
            }
        }

        return len > 0 || startIndex >= endIndex;
    }

    public boolean receiveResponse(AsyncSocketTransport pcon, AsyncHandler<RedisCommandList> presponseHandler) {
        con = pcon;
        individualResponseHandler.responseIndex = startIndex;
        individualResponseHandler.responseHandler = presponseHandler;
        RedisCommandInfo commandInfo = null;
        while(individualResponseHandler.responseIndex < endIndex && (commandInfo  = commandObjects[individualResponseHandler.responseIndex].receiveResponse(pcon, individualResponseHandler)) != null) {
            individualResponseHandler.responseIndex++;
        }

        return commandInfo != null || startIndex >= endIndex;
    }

    public void sendFinalResponse(AsyncRedisConnection paredis, Executor executor, boolean forceCallbackViaExecutor) {
        aredis = paredis;
        if(futureResults != null) {
            futureResults.run();
        }
        else if(futureResult != null) {
            futureResult.run();
        }
        if(finalResponsesHandler != null || finalResponseHandler != null) {
            FinalResponseSender finalResponseSender = new FinalResponseSender();
            if(!isSyncCallback || forceCallbackViaExecutor) {
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
        } else {
            aredis.updatePipelineSizeForResponse(this);
        }
    }

    public int updateNumPipelinedCommands(int updatedCommandLen) {
        int remain = updatedCommandLen;
        while(numPipelinedCammands < endIndex) {
            int nextRequestDataLen = commandObjects[numPipelinedCammands].requestDataLength;
            if(nextRequestDataLen <= remain) {
                remain -= nextRequestDataLen;
                numPipelinedCammands++;
            }
            else {
                break;
            }
        }

        return remain;
    }

    public String validateCommandList(ConnectionType connectionType) {
        String msg = null;
        int i;
        boolean isMulti = false;
        if(commandObjects.length == 0) {
            msg = "List of Commands is Empty";
        }
        for(i = 0; i < commandObjects.length; i++) {
            RedisCommandInfo commandInfo = commandObjects[i].commandInfo;
            Object[] params = commandInfo.getParams();
            RedisCommand command = commandInfo.getCommand();
            if(commandInfo.runStatus != CommandStatus.SKIPPED) {
                msg = "This commandInfo with run status " + commandInfo.runStatus + " seems to have been already submitted. Resubmit commandInfos only if they have status SKIPPED. " + command + ' ' + params;
                break;
            }
            if(command.isScriptCommand()) {
                hasScriptCommands = true;
            }
            if(command == RedisCommand.SUBSCRIBE || command == RedisCommand.PSUBSCRIBE || command == RedisCommand.UNSUBSCRIBE || command == RedisCommand.PUNSUBSCRIBE) {
                msg = "" + command + " not allowed with AsyncRedisConnection. Please use RedisSubscription class for subscribing to/unsubscribing message channels";
            }
            if(command == RedisCommand.MULTI) {
                if(params.length == 0 && !isMulti) {
                    isMulti = true;
                }
            }
            else if(command == RedisCommand.EXEC) {
                if(params.length == 0 && isMulti) {
                    isMulti = false;
                }
            }
            else if(command == RedisCommand.SELECT) {
                if(connectionType == ConnectionType.BORROWED) {
                    if(getDbIndex(params) >= 0) {
                        hasSelectCommands = true;
                    }
                }
            }
            else if(command.isBlocking() || command.isStateful()) {
                if(connectionType == ConnectionType.SHARED) {
                    msg = "Command " + command + " is blocking or stateful and not allowed on a shared connection. Please use AsyncRedisConnectionPool or mark this connection as standalone using setConnectionType";
                    break;
                }
            }
        }
        if(msg == null && isMulti && connectionType != ConnectionType.STANDALONE) {
            msg = "MULTI without matching EXEC is allowed only on a standalone connection";
        }
        return msg;
    }

    private int getDbIndex(Object params[]) {
        int i, startIndex = 0, dbi = -1, len;
        if(params.length == 1) {
            String s = params[0].toString();
            char ch;
            boolean negate = false;
            if((len = s.length()) > 0 && ((ch = s.charAt(0)) == '+' || ch == '-')) {
                startIndex++;
                if(ch == '-') {
                    negate = true;
                }
            }
            if(startIndex < len) {
                ch = s.charAt(startIndex);
                if(Character.isDigit(ch)) {
                    if(!negate) {
                        i = startIndex;
                        do {
                            i++;
                        } while(i < len && Character.isDigit(s.charAt(i)));
                        if(i - startIndex < 2) {
                            dbi = Integer.parseInt(s.substring(startIndex, i));
                        }
                    }
                }
                else {
                    dbi = 0;
                }
            }
            else if(startIndex > 0) {
                dbi = 0;
            }
        }
        return dbi;
    }

    public int checkSelectCommands(int dbIndex, int currentDbIndex) {
        int i, len = commandObjects.length, dbi, requiredDbIndex = dbIndex, startDbIndex = dbIndex, startIdx = 0, endIdx;
        RedisCommandInfo commandInfo = commandObjects[0].commandInfo;
        Object[] params = commandInfo.getParams();
        RedisCommand command = commandInfo.getCommand();
        if(command == RedisCommand.SELECT && (dbi = getDbIndex(params)) >= 0) {
            startDbIndex = dbi;
            if(startDbIndex == currentDbIndex) {
                startIdx = 1;
                commandInfo.resultType = ResultType.STRING;
                commandInfo.result = "+OK";
                commandInfo.runStatus = CommandStatus.SUCCESS;
            }
            else {
                requiredDbIndex = -(dbi + 1);
            }
        }
        startIndex = startIdx;
        numPipelinedCammands = startIdx;
        for(i = len; i > 1;) {
            i--;
            commandInfo = commandObjects[i].commandInfo;
            params = commandInfo.getParams();
            command = commandInfo.getCommand();
            if(command == RedisCommand.SELECT && getDbIndex(params) >= 0) {
                commandInfo.resultType = ResultType.STRING;
                commandInfo.result = "+OK";
                commandInfo.runStatus = CommandStatus.SUCCESS;
            }
            else {
                i++;
                break;
            }
        }
        endIdx = i;
        endIndex = endIdx;
        int lastDbIndex = startDbIndex;
        int lastMultiDbIndex = startDbIndex;
        boolean isMulti = false;
        for(i = startIdx; i < endIdx; i++) {
            commandInfo = commandObjects[i].commandInfo;
            params = commandInfo.getParams();
            command = commandInfo.getCommand();
            if(command == RedisCommand.SELECT) {
                if((dbi = getDbIndex(params)) >= 0) {
                    if(isMulti) {
                        lastMultiDbIndex = dbi;
                    }
                    else {
                        lastDbIndex = dbi;
                        lastMultiDbIndex = dbi;
                    }
                }
            }
            else {
                if(command == RedisCommand.MULTI) {
                    if(params.length == 0 && !isMulti) {
                        isMulti = true;
                    }
                }
                else if(command == RedisCommand.EXEC) {
                    if(params.length == 0 && isMulti) {
                        isMulti = false;
                    }
                }
            }
        }
        if(lastDbIndex != lastMultiDbIndex) {
            lastDbIndex = -1;
        }
        finalDbIndex = lastDbIndex;

        return requiredDbIndex;
    }

    public void updateScriptStatuses(ScriptStatuses scriptStatuses) {
        if(hasScriptCommands) {
            int [] flags = null;
            for(int i = 0; i < commandObjects.length; i++) {
                RedisCommandInfo commandInfo = commandObjects[i].commandInfo;
                Object[] params = commandInfo.getParams();
                RedisCommand command = commandInfo.getCommandRun();
                if (params.length > 0 && command.isScriptCommand() && commandInfo.runStatus == CommandStatus.SUCCESS) {
                    ResultType resultType = commandInfo.resultType;
                    if (command == RedisCommand.EVALSHA && commandInfo.getCommand() == RedisCommand.EVALCHECK && (resultType != ResultType.REDIS_ERROR || ((String) commandInfo.getResult()).indexOf("NOSCRIPT") < 0)) {
                        // We can continue since this means the script has already been checked to be loaded
                        continue;
                    }
                    Object p = null;
                    Script s = null;
                    String scriptCommand = null;
                    if (command == RedisCommand.SCRIPT) {
                        scriptCommand = (String) params[0];
                        if (resultType != ResultType.REDIS_ERROR) {
                            if ("EXISTS".equalsIgnoreCase(scriptCommand)) {
                                Object [] loadStatuses = (Object[]) commandInfo.getResult();
                                for(int paramIndex = 1; paramIndex < params.length; paramIndex++) {
                                    p = params[paramIndex];
                                    if (p instanceof Script) {
                                        s = (Script) p;
                                        if ("1".equals(loadStatuses[paramIndex - 1])) {
                                            flags = scriptStatuses.setLoaded(flags, s, true);
                                        } else if (scriptStatuses.clearLoadStatusesIfLoaded(s)) {
                                            flags = new int[0];
                                            break;
                                        }
                                    }
                                }
                                continue;
                            } else if ("FLUSH".equalsIgnoreCase((String) params[0])) {
                                scriptStatuses.clearLoadStatuses();
                                flags = new int[0];
                                continue;
                            }
                        }
                        if (params.length < 2) {
                            continue;
                        }
                        p = params[1];
                    } else {
                        p = params[0];
                    }
                    if (p instanceof Script) {
                        s = (Script) p;
                        Boolean loadStatus = null;
                        if (command == RedisCommand.EVALSHA) {
                            loadStatus = resultType != ResultType.REDIS_ERROR || ((String) commandInfo.getResult()).indexOf("NOSCRIPT") < 0;
                        } else if (command == RedisCommand.EVAL) {
                            loadStatus = true;
                        } else if (command == RedisCommand.SCRIPT) {
                            if ("LOAD".equalsIgnoreCase(scriptCommand)) {
                                if (resultType != ResultType.REDIS_ERROR) {
                                    loadStatus = true;
                                }
                            }
                        }
                        if (loadStatus != null) {
                            if (loadStatus) {
                                flags = scriptStatuses.setLoaded(flags, s, true);
                            } else {
                                // When we encounter any script which is not loaded but tagged as loaded in
                                // ScriptStatuses we clear all flags since it means that SCRIPT FLUSH is run
                                // or another Redis Server has taken the existing servers place
                                if (scriptStatuses.clearLoadStatusesIfLoaded(s)) {
                                    flags = new int[0];
                                }
                            }
                        }
                    }
                }
            }

        }
    }
}
