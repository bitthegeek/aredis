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
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.cache.RedisCommandInfo.CommandStatus;
import org.aredis.cache.RedisCommandInfo.ResultType;
import org.aredis.io.CompressibleByteArrayOutputStream;
import org.aredis.io.RedisConstants;
import org.aredis.net.AsyncSocketTransport;
import org.aredis.net.ServerInfo;

class RedisCommandObject implements AsyncHandler<RedisRawResponse> {
    private static final Log log = LogFactory.getLog(RedisCommandObject.class);

    private static final byte [] filler = new byte[7];

    /*
    class SubscriptionFutureTask extends FutureTask<SubscriptionInfo> implements Runnable {
        SubscriptionInfo subscriptionInfo;

        public SubscriptionFutureTask(SubscriptionInfo psubscriptionInfo) {
            super(EmptyRunnable.instance, psubscriptionInfo);
            subscriptionInfo = psubscriptionInfo;
        }

    }
    */

    RedisCommandInfo commandInfo;

    byte [] requestData;

    int requestDataLength;

    private AsyncHandler<RedisCommandInfo> responseHandler;

    private AsyncSocketTransport con;

    private Queue<RedisCommandInfo> queuedMultiCommands;

    SubscriptionInfo subscriptionInfo;

    public RedisCommandObject(RedisCommandInfo pcommandInfo, Queue<RedisCommandInfo> pqueuedMultiCommands) {
        commandInfo = pcommandInfo;
        queuedMultiCommands = pqueuedMultiCommands;
    }

    private void writeArg(CompressibleByteArrayOutputStream bop, Object param, boolean isData, DataHandler dataHandler, ServerInfo serverInfo) throws IOException {
        boolean isInfo = log.isInfoEnabled();
        bop.setCompressionEnabled(false);
        int savedCompressionThreshold = bop.getCompressionThreshold();
        if(!isData && param instanceof Number) {
            param = param.toString();
        }
        byte [] commandOrKeyOrParamBytes = null;
        if(!isData || dataHandler == null) {
            dataHandler = AsyncRedisConnection.KEY_HANDLER;
            if (param instanceof String) {
                commandOrKeyOrParamBytes = ((String) param).getBytes(RedisConstants.UTF_8_CHARSET);
            } else {
                commandOrKeyOrParamBytes = (byte[]) param;
            }
        }
        int i, len = filler.length;
        if (commandOrKeyOrParamBytes != null) {
            i = commandOrKeyOrParamBytes.length;
            len = 1;
            while (i >= 10) {
                len++;
                i /= 10;
            }
            len += 3;
        }
        int numPos = bop.getCount();
        bop.write(filler, 0, len);
        int curDataStart = bop.getCount();
        // Write the data after the filler
        try {
            if (commandOrKeyOrParamBytes != null) {
                bop.write(commandOrKeyOrParamBytes);
            } else {
                dataHandler.serialize(param, commandInfo.metaData, bop, serverInfo);
            }
            bop.close();
            if(isInfo) {
                String msg = bop.getCompressionInfo();
                if(msg != null) {
                    log.info(dataHandler.getClass().getName() + ": " + msg);
                }
            }
            bop.clearCompressionInfo();
        }
        finally {
            bop.setCompressionEnabled(false);
            bop.setCompressionThreshold(savedCompressionThreshold);
        }
        // Write out the count in filler area and move back data
        int dataLen = bop.getCount() - curDataStart;
        // Compute actual bytes to print dataLen with markers
        byte[] dataLenAsBytes = String.valueOf(dataLen).getBytes("UTF-8");
        int argSizeInfoLen = dataLenAsBytes.length + 3;
        int newDataStart = curDataStart;
        int diff = argSizeInfoLen - len;
        if(diff != 0) {
            if (commandOrKeyOrParamBytes != null) {
                log.error("Unexpected Param Len Descripency " + diff + " org = " + len);
            }
            while(diff > 0) {
                len = diff;
                if(len > filler.length) {
                    len = filler.length;
                }
                bop.write(filler, 0, len);
                diff -= len;
            }
            byte [] buf = bop.getBuf();
            newDataStart = numPos + argSizeInfoLen;
            System.arraycopy(buf, curDataStart, buf, newDataStart, dataLen);
        }
        bop.setCount(numPos);
        bop.write('$');
        bop.write(dataLenAsBytes);
        bop.write('\r');
        bop.write('\n');
        bop.setCount(newDataStart + dataLen);
        bop.write('\r');
        bop.write('\n');
    }

    public void generateRequestData(ServerInfo sInfo) throws IOException {
        if(commandInfo.debugBuf != null) {
            commandInfo.addDebug("Going to Generate Request Data");
        }
        if(commandInfo.serverInfo == null) {
            commandInfo.serverInfo = sInfo;
        }
        Script s;
        Object[] params = commandInfo.getParams();
        RedisCommand command = commandInfo.getCommand();
        if (command == RedisCommand.EVALCHECK) {
            // Use EVALSHA, Check and Load will be done during sendRequest
            command = RedisCommand.EVALSHA;
        }
        commandInfo.commandRun = command;
        DataHandler dataHandler = commandInfo.getDataHandler();
        Object param = command.name();
        char[] argTypes = command.argTypes;
        int repeatableFromIndex = command.getRepeatableFromIndex();
        CompressibleByteArrayOutputStream bop = new CompressibleByteArrayOutputStream();
        int i = -1;
        int argTypeIndex = -1;
        int nextArgCount = 0;
        boolean isData = false, nextIsData = false;
        DataHandler dh = null, nextDh = null;

        int paramCount = 0;
        if(params != null) {
            paramCount = params.length;
        }
        bop.setCompressionEnabled(false);
        bop.write('*');
        bop.write(String.valueOf(paramCount + 1).getBytes("UTF-8"));
        bop.write('\r');
        bop.write('\n');
        do {
            writeArg(bop, param, isData, dh, sInfo);
            i++;
            if(i < paramCount) {
                param = params[i];
                if (param instanceof Script) {
                    s = (Script) param;
                    switch (command) {
                      case EVAL:
                        param = s.getScript();
                        break;
                      case EVALSHA:
                        param = s.getSha1sum();
                        break;
                      case SCRIPT:
                        if ("EXISTS".equalsIgnoreCase(params[0].toString())) {
                            param = s.getSha1sum();
                        } else {
                            param = s.getScript();
                        }
                        break;
                      default:
                        break;
                    }
                }
                if(nextArgCount > 0) {
                    nextArgCount--;
                    dh = nextDh;
                    isData = nextIsData;
                }
                else {
                    isData = false;
                    dh = null;
                    argTypeIndex++;
                    if(argTypeIndex >= argTypes.length) {
                        argTypeIndex = repeatableFromIndex;
                        if(repeatableFromIndex < 0) {
                            throw new IllegalArgumentException("Extra parameter '" + params[i] + "' at position " + (i + 1) + " for command " + command + ". Only " + argTypes.length + " parameter(s) expected.");
                        }
                    }
                    char argType = argTypes[argTypeIndex];
                    if(argType == 'c') {
                        nextArgCount = Integer.parseInt(param.toString());
                        argTypeIndex++;
                        nextIsData = false;
                        nextDh = null;
                        argType = argTypes[argTypeIndex];
                        if(argType == 'v') {
                            nextIsData = true;
                            nextDh = dataHandler;
                        }
                    }
                    else if(argType == 'v') {
                        isData = true;
                        dh = dataHandler;
                    }
                }
            }
        } while(i < paramCount);
        requestData = bop.getBuf();
        requestDataLength = bop.getCount();

        if(commandInfo.debugBuf != null) {
            commandInfo.addDebug("Generated Request Data of length: " + requestDataLength);
        }
    }

    public int sendRequest(AsyncSocketTransport con, AsyncHandler<Integer> requestHandler, ScriptStatuses scriptStatuses) {
        boolean isDebug = log.isDebugEnabled();
        if(isDebug) {
            log.debug("Sending Request: " + commandInfo.getCommand() + ' ' + commandInfo.getParams());
        }
        Script s;
        Object[] params = commandInfo.getParams();
        RedisCommand command = commandInfo.getCommand();
        if (command == RedisCommand.EVALCHECK && params.length > 0) {
            int [] statusFlags = scriptStatuses.getStatusFlags();
            s = (Script) params[0];
            boolean isLoaded = ScriptStatuses.isLoaded(statusFlags, s);
            // The Below If block is optional. It attempts to check and load the script synchronously using the common
            // Redis connection. If the below block is not there EVALCHECK on an unloaded script will use EVAL
            // instead of EVALSHA and the script will be eventually tagged as loaded when the EVAL completes.
            // However many EVAL commands for the same script could enter the pipeline before the first one
            // completes.
            if (!isLoaded) {
                AsyncRedisConnection commonAredis = RedisServerWideData.getInstance(con).getCommonAredisConnection(null, 0);
                synchronized (commonAredis) {
                    statusFlags = scriptStatuses.getStatusFlags();
                    isLoaded = ScriptStatuses.isLoaded(statusFlags, s);
                    if (!isLoaded) {
                        try {
                            Object [] result = (Object[]) commonAredis.submitCommand(new RedisCommandInfo(RedisCommand.SCRIPT, "EXISTS", s)).get().getResult();
                            String debugStr = "";
                            if (result != null && result.length > 0) {
                                if (!"1".equals(result[0])) {
                                    debugStr = "LOAD CALLED ";
                                    // Attempt to load it
                                    commonAredis.submitCommand(new RedisCommandInfo(RedisCommand.SCRIPT, "LOAD", s)).get(5, TimeUnit.SECONDS);
                                }
                            }
                            // If the above check and load attempt was successful the statusFlags would have
                            // got updated, so just pick the loaded status from the status flags
                            statusFlags = scriptStatuses.getStatusFlags();
                            isLoaded = ScriptStatuses.isLoaded(statusFlags, s);
                            if (isDebug) {
                                log.debug(debugStr + "UPDATED ISLOADED " + isLoaded + " FOR scriptIndex " + s.getIndex() + " SERVERINFO " + con + " SERVERINDEX " + con.getServerIndex());
                            }
                        } catch (Exception e) {
                            log.error("Error Trying to Load Script: " + s.getScript(), e);
                        }
                    }
                }
            }

            if (!isLoaded) {
                command = RedisCommand.EVAL;
                // Hacky replacement of EVALSHA and the sha1sum by script in the final
                // serialized data. However EVAL command is not normally expected since we
                // do an EXISTS check and LOAD. So it is Ok.
                int pos = 1;
                do {
                    pos++;
                } while (requestData[pos] != '\r' || requestData[pos+1] != '\n');
                pos += 2;
                byte [] scriptBytes = s.getScript().getBytes(RedisConstants.UTF_8_CHARSET);
                byte [] sha1sumBytes = s.getSha1sum().getBytes(RedisConstants.UTF_8_CHARSET);
                int orgParamOffset = pos + ("$7\r\nEVALSHA\r\n$" + sha1sumBytes.length + "\r\n").getBytes(RedisConstants.UTF_8_CHARSET).length + sha1sumBytes.length;
                byte [] newPrefixData = ("$4\r\nEVAL\r\n$" + scriptBytes.length + "\r\n").getBytes(RedisConstants.UTF_8_CHARSET);
                int newParamOffset = pos + newPrefixData.length + scriptBytes.length;
                int newRequestDataLength = requestDataLength + newParamOffset - orgParamOffset;
                byte [] newRequestData = requestData;
                if (newRequestData.length < newRequestDataLength) {
                    newRequestData = new byte[newRequestDataLength];
                }
                System.arraycopy(requestData, orgParamOffset, newRequestData, newParamOffset, requestDataLength - orgParamOffset);
                System.arraycopy(scriptBytes, 0, newRequestData, pos + newPrefixData.length, scriptBytes.length);
                System.arraycopy(newPrefixData, 0, newRequestData, pos, newPrefixData.length);
                System.arraycopy(requestData, 0, newRequestData, 0, pos);
                requestData = newRequestData;
                requestDataLength = newRequestDataLength;
                commandInfo.commandRun = command;
            }
            if (isDebug && commandInfo.getCommand() != commandInfo.commandRun) {
                log.debug("Original Command = " + commandInfo.getCommand() + " Command Run = " + commandInfo.commandRun);
            }
        }
        int synchronousBytesSent = con.write(requestData, 0, requestDataLength, requestHandler);
        return synchronousBytesSent;
    }

    public RedisCommandInfo receiveResponse(AsyncSocketTransport pcon, AsyncHandler<RedisCommandInfo> presponseHandler) {
        responseHandler = presponseHandler;
        con = pcon;
        RedisResponseReader responseReader = new RedisResponseReader(con);
        RedisRawResponse response = responseReader.readResponse(this);
        RedisCommandInfo retVal = null;
        if(response != null) {
           completed(response, null, false);
           retVal = commandInfo;
        }

        return retVal;
    }

    private void convertToCommandInfo(RedisRawResponse response, RedisCommandInfo redisCommandInfo, Throwable e) {
        ResultType resultType = response.getResultType();
        redisCommandInfo.resultType = resultType;
        redisCommandInfo.runStatus = CommandStatus.SUCCESS;
        redisCommandInfo.error = e;
        Object rawResult = response.getResult();
        if(response.isError()) {
            redisCommandInfo.runStatus = CommandStatus.NETWORK_ERROR;
        }
        else {
            RedisCommand command = redisCommandInfo.getCommand();
            if(rawResult != null) {
                if(resultType == ResultType.MULTIBULK) {
                    RedisRawResponse rawResults[] = (RedisRawResponse []) rawResult;
                    Object results[] = new Object[rawResults.length];
                    int i;
                    if(command == RedisCommand.EXEC) {
                        for(i = 0; i < rawResults.length; i++) {
                            RedisCommandInfo subCommandInfo;
                            RedisCommandInfo queuedCommand = queuedMultiCommands.poll();
                            if(queuedCommand != null) {
                                subCommandInfo = new RedisCommandInfo(queuedCommand.getDataHandler(), queuedCommand.getCommand(), queuedCommand.getParams());
                            }
                            else {
                                subCommandInfo = new RedisCommandInfo(null);
                                log.error("Could not find queued command for exec");
                            }
                            RedisRawResponse nextRawResult = rawResults[i];
                            convertToCommandInfo(nextRawResult, subCommandInfo, null);
                            results[i] = subCommandInfo;
                        }
                        if(queuedMultiCommands.size() != 0) {
                            StringBuilder sb = new StringBuilder();
                            for(RedisCommandInfo commandInfo : queuedMultiCommands) {
                                sb.append(commandInfo.getCommand()).append(", ");
                            }
                            sb.setLength(sb.length() - 2);
                            log.error("Following Queued commands not returned in EXEC: " + sb.toString());
                        }
                    }
                    else {
                        ResultTypeInfo [] mbResultTypes = new ResultTypeInfo[rawResults.length];
                        redisCommandInfo.mbResultTypes = mbResultTypes;
                        // Regular Multibulk
                        for(i = 0; i < rawResults.length; i++) {
                            RedisRawResponse nextRawResult = rawResults[i];
                            if(nextRawResult != null) {
                                ResultType nextResultType = nextRawResult.getResultType();
                                if(nextResultType == ResultType.MULTIBULK) {
                                    RedisCommandInfo subCommandInfo = new RedisCommandInfo(redisCommandInfo.dataHandler, null);
                                    convertToCommandInfo(nextRawResult, subCommandInfo, null);
                                    mbResultTypes[i] = new ResultTypeInfo(nextResultType, subCommandInfo.mbResultTypes);
                                    results[i] = subCommandInfo;
                                } else {
                                    mbResultTypes[i] = new ResultTypeInfo(nextResultType, null);
                                    results[i] = nextRawResult.getResult();
                                }
                            }
                            else {
                                log.error("Unexpected NULL Result Type for command " + commandInfo.getCommand() + " result no: " + i);
                            }
                        }
                    }
                    redisCommandInfo.result = results;
                }
                else {
                    redisCommandInfo.result = rawResult;
                    if(resultType == ResultType.STRING && "QUEUED".equals(rawResult)) {
                        queuedMultiCommands.add(redisCommandInfo);
                    }
                }
            }
            if((command == RedisCommand.MULTI && resultType != ResultType.REDIS_ERROR || (command == RedisCommand.EXEC || command == RedisCommand.DISCARD) && redisCommandInfo.getParams().length == 0)) {
                queuedMultiCommands.clear();
            }
        }
    }

    private void completed(RedisRawResponse result, Throwable e, boolean callback) {
        convertToCommandInfo(result, commandInfo, e);
        if(callback) {
            responseHandler.completed(commandInfo, e);
        }
    }

    @Override
    public void completed(RedisRawResponse result, Throwable e) {
        completed(result, e, true);
    }

    RedisCommandInfo getCommandInfo() {
        return commandInfo;
    }

}
