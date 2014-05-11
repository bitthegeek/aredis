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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.net.ServerInfo;

/**
 * This is a class to Hold a Redis Command with its arguments and also the result once the command is completed.
 * RedisCommandInfo Objects should be created for every command submission and not re-used.
 * @author Suresh
 *
 */
public class RedisCommandInfo {

    private static final Log log = LogFactory.getLog(RedisCommandInfo.class);

    /**
     * Enumerates the Result Types of Redis
     * @author Suresh
     *
     */
    public static enum ResultType {STRING, INT, BULK, MULTIBULK, REDIS_ERROR};

    /**
     * Enumerates the CommandStatus which is expected to be SUCCESS on normal execution.
     * @author Suresh
     *
     */
    public static enum CommandStatus {
        /**
         * Indicates that the Command has run successfully. This is so even if there is an error
         * in the command usage. Such Syntax errors are indicated by the ResultType REDIS_ERROR.
         */
        SUCCESS,
        /**
         * Indicates that the command was not sent because the connection is DOWN.
         */
        SKIPPED,
        /**
         * Indicates that there was an error during de-serialization of the data by the Data Handler.
         */
        DECODE_ERROR,
        /**
         * Indicates that there was an error (most likely a network error) during command it is not known whether the command was executed or not.
         */
        NETWORK_ERROR
    }

    private RedisCommand command;

    RedisCommand commandRun;

    private Object [] params;

    Object metaData;

    ResultType resultType;

    Object result;

    ResultTypeInfo [] mbResultTypes;

    CommandStatus runStatus;

    Throwable error;

    DataHandler dataHandler;

    StringBuffer debugBuf;

    ServerInfo serverInfo;

    private boolean isDeserialized;

    // Extra fields for ShardedClient with split keys
    RedisCommandInfo splitCommands[];

    // Index of splitCommand in connections array
    int connectionIndex = -1;

    // Collection used to aggregate results like Set
    Object aggregationResult;

    private void deserializeShardedCommandInfo() {
        int i;
        for(i = 0; i < splitCommands.length; i++) {
            splitCommands[i].deserialize();
        }
        command.shardedResultHandler.aggregateResults(this);
    }

    private void deserializeCommandInfo() {
        int i;
        if(result != null && dataHandler != null) {
            byte rawData[] = null;
            if(resultType == ResultType.BULK) {
                rawData = (byte[]) result;
                try {
                    result = dataHandler.deserialize(metaData, rawData, 0, rawData.length, serverInfo);
                } catch (Exception e) {
                    result = null;
                    if(runStatus == CommandStatus.SUCCESS) {
                        runStatus = CommandStatus.DECODE_ERROR;
                    }
                    log.error("Error Deserializing response from command: " + command, e);
                }
            }
            else if(resultType == ResultType.MULTIBULK) {
                Object mbArray[] = (Object []) result;
                if(command != RedisCommand.EXEC) {
                    for(i = 0; i < mbArray.length; i++) {
                        ResultType nextResultType = mbResultTypes[i].getResultType();
                        if (nextResultType == ResultType.MULTIBULK) {
                            RedisCommandInfo nextMbResult = (RedisCommandInfo) mbArray[i];
                            nextMbResult.deserialize();
                            mbArray[i] = nextMbResult.result;
                        } else if(nextResultType == ResultType.BULK && (rawData = (byte[]) mbArray[i]) != null) {
                            try {
                                mbArray[i] = dataHandler.deserialize(metaData, rawData, 0, rawData.length, serverInfo);
                            } catch (Exception e) {
                                if(runStatus == CommandStatus.SUCCESS) {
                                    runStatus = CommandStatus.DECODE_ERROR;
                                }
                                mbArray[i] = null;
                                log.error("Error Deserializing response from command: " + command, e);
                            }
                        }
                    }
                }
                else {
                    for(i = 0; i < mbArray.length; i++) {
                        RedisCommandInfo nextMultiResult = (RedisCommandInfo) mbArray[i];
                        nextMultiResult.deserialize();
                    }
                }
            }
        }
    }

    private void deserialize() {
        if(!isDeserialized) {
            isDeserialized = true;
            if(splitCommands == null) {
                deserializeCommandInfo();
            }
            else {
                deserializeShardedCommandInfo();
            }
        }
    }

    /**
     * Utility to add a Debug message to the Command. setDebugBuf should have been called to set a StringBuffer for this.
     * @param message Message to append. A new line is added after the message.
     */
    public void addDebug(String message) {
        if(debugBuf != null) {
            debugBuf.append(message).append('\n');
        }
    }

    /**
     * Creates a Redis  CommandInfo Object
     * @param pdataHandler Data Handler to use
     * @param pmetaData Meta Data for the Data handler if required
     * @param pcommand The Redis Command to run
     * @param pparams Parameters for the Redis Command
     */
    public RedisCommandInfo(DataHandler pdataHandler, Object pmetaData, RedisCommand pcommand, Object ... pparams) {
        command = pcommand;
        params = pparams;
        dataHandler = pdataHandler;
        metaData = pmetaData;
    }

    /**
     * Creates a Redis  CommandInfo Object
     * @param pdataHandler Data Handler to use
     * @param pcommand The Redis Command to run
     * @param pparams Parameters for the Redis Command
     */
    public RedisCommandInfo(DataHandler pdataHandler, RedisCommand pcommand, Object ... pparams) {
        this(pdataHandler, null, pcommand, pparams);
    }

    /**
     * Creates a Redis  CommandInfo Object which uses the Default Data Handler of the {@link AsyncRedisConnection}.
     * @param pmetaData Meta Data for the Data handler if required
     * @param pcommand The Redis Command to run
     * @param pparams Parameters for the Redis Command
     */
    public RedisCommandInfo(Object pmetaData, RedisCommand pcommand, Object ... pparams) {
        this(null, pmetaData, pcommand, pparams);
    }

    /**
     * Creates a Redis  CommandInfo Object which uses the Default Data Handler of the {@link AsyncRedisConnection}.
     * @param pcommand The Redis Command to run
     * @param pparams Parameters for the Redis Command
     */
    public RedisCommandInfo(RedisCommand pcommand, Object ... pparams) {
        this(null, null, pcommand, pparams);
    }

    /**
     * Gets the Type of the Result. This will be null if the run status returned by getRunStatus is not SUCCESS.
     * @return The Result Type
     */
    public ResultType getResultType() {
        return resultType;
    }

    /**
     * Gets the result. For resultTypes other than BULK and MULTIBULK the value returned is a String.
     * For resultType BULK the appropriate Data Handler is used to de-serialize the response. In case of the resultType
     * MULTIBULK the return value is an Array of Objects which could have arrays in case of a nested MULTIBULK result
     * as in the response of a SCAN command. The DataHandler is used to de-serialize individual bulk responses
     * in a MULTIBULK response. In case of MULTIBULK response of an EXEC command the Result is an Array of
     * RedisCommandInfo and each of the elements of the Array has the response for the corresponding command in the
     * Redis Transaction.
     * @return Result Appropriate result which could be null in case of null MULTIBULK response or if getRunStatus
     * returns SKIPPED or NETWORK_ERROR
     */
    public Object getResult() {
        if(!isDeserialized && runStatus == CommandStatus.SUCCESS) {
            deserialize();
        }
        return result;
    }

    /**
     * In case the ResultType returned by getResultType is MULTIBULK this method returns the ResultTypes in
     * the response. The response is an Array of ResultTypeInfo rather than ResultType because some of the
     * ResultTypes may again be MULTIBULK as in the response of a SCAN command in which case the corresponding
     * ResultTypeInfo.getMbResultTypes gets the ResultTypes of those sub-MULTIBULK responses.
     *
     * This info is usually not needed by the user of aredis since he already knows the MULTIBULK response for
     * the command he/she has sent. However aredis also uses this info when de-serializing.
     *
     * Note that the return value is NULL for an EXEC command.
     *
     * @return An array identifying the individual ResultTypes if the ResultType is MULTIBULK, null otherwise
     */
    public ResultTypeInfo[] getMbResultTypes() {
        return mbResultTypes;
    }

    /**
     * Converts any result Object to an int
     * @param res Result Object
     * @param def Default value to return in case the result is null or not an Integer
     * @return Result as int
     */
    public static int getIntResult(Object res, int def) {
        int intResult = def;
        if(res != null) {
            try {
                intResult = Integer.parseInt((String) res);
            }
            catch(Exception e) {
                log.warn("Error Returning " + res + " as int. Returning default of " + def, e);
            }
        }
        return intResult;
    }

    /**
     * Converts the result to an int
     * @param def Default value to return in case the result is null or not an Integer
     * @return Result as int
     */
    public int getIntResult(int def) {
        return getIntResult(getResult(), def);
    }

    /**
     * Converts any result Object to a long
     * @param res Result Object
     * @param def Default value to return in case the result is null or not a Long
     * @return Result as long
     */
    public long getLongResult(Object res, long def) {
        long longResult = def;
        if(res != null) {
            try {
                longResult = Long.parseLong((String) res);
            }
            catch(Exception e) {
                log.warn("Error Returning " + res + " as long. Returning default of " + def, e);
            }
        }
        return longResult;
    }

    /**
     * Converts the result to a long
     * @param def Default value to return in case the result is null or not a Long
     * @return Result as long
     */
    public long getLongResult(long def) {
        return getLongResult(getResult(), def);
    }

    /**
     * Converts any result Object to a short value
     * @param res Result Object
     * @param def Default value to return in case the result is null or not a Short int
     * @return Result as a short value
     */
    public short getShortResult(Object res, short def) {
        short intResult = def;
        if(res != null) {
            try {
                intResult = Short.parseShort((String) res);
            }
            catch(Exception e) {
                log.warn("Error Returning " + res + " as short. Returning default of " + def, e);
            }
        }
        return intResult;
    }

    /**
     * Converts the result to a short value
     * @param def Default value to return in case the result is null or not a Short int
     * @return Result as a short value
     */
    public short getShortResult(short def) {
        return getShortResult(getResult(), def);
    }

    /**
     * Converts any result Object to a byte
     * @param res Result Object
     * @param def Default value to return in case the result is null or not a Byte
     * @return Result as a byte
     */
    public byte getByteResult(Object res, byte def) {
        byte byteResult = def;
        if(res != null) {
            try {
                byteResult = Byte.parseByte((String) res);
            }
            catch(Exception e) {
                log.warn("Error Returning " + res + " as byte. Returning default of " + def, e);
            }
        }
        return byteResult;
    }

    /**
     * Converts the result to a byte
     * @param def Default value to return in case the result is null or not a Byte
     * @return Result as a byte
     */
    public byte getByteResult(byte def) {
        return getByteResult(getResult(), def);
    }

    /**
     * Converts any result Object to a double value
     * @param res Result Object
     * @param def Default value to return in case the result is null or not a Double
     * @return Result as a double value
     */
    public double getDoubleResult(Object res, double def) {
        double doubleResult = def;
        if(res != null) {
            try {
                doubleResult = Double.parseDouble((String) res);
            }
            catch(Exception e) {
                log.warn("Error Returning " + res + " as double. Returning default of " + def, e);
            }
        }
        return doubleResult;
    }

    /**
     * Converts the result to a double value
     * @param def Default value to return in case the result is null or not a Double
     * @return Result as a double value
     */
    public double getDoubleResult(double def) {
        return getDoubleResult(getResult(), def);
    }

    /**
     * Converts any result Object to a float
     * @param res Result Object
     * @param def Default value to return in case the result is null or not a Float
     * @return Result as a float
     */
    public float getFloatResult(Object res, float def) {
        float floatResult = def;
        if(res != null) {
            try {
                floatResult = Float.parseFloat((String) res);
            }
            catch(Exception e) {
                log.warn("Error Returning " + res + " as float. Returning default of " + def, e);
            }
        }
        return floatResult;
    }

    /**
     * Converts the result to a float
     * @param def Default value to return in case the result is null or not a Float
     * @return Result as a float
     */
    public float getFloatResult(float def) {
        return getFloatResult(getResult(), def);
    }

    /**
     * Gets the command
     * @return the command
     */
    public RedisCommand getCommand() {
        return command;
    }

    /**
     * Returns the actual command that was attempted to run.
     * This is filled only when the command is submitted and is the same as command except when
     * command is the pseudo-command {@link RedisCommand#EVALCHECK} in which case it is either EVAL
     * or EVALSHA.
     * @return The actual command run which could be different than getCommand for EVALCHECK
     */
    public RedisCommand getCommandRun() {
        return commandRun;
    }

    /**
     * Gets the command parameters
     * @return command parameters
     */
    public Object[] getParams() {
        return params;
    }

    /**
     * Gets the data handler
     * @return data handler that is in use
     */
    public DataHandler getDataHandler() {
        return dataHandler;
    }

    /**
     * Gets the Run Status to indicate whether the command ran successfully or if there was an error.
     * Note that Redis Command errors are indicated by a RunStatus of SUCCESS. Only the ResultType indicates a
     * REDIS_ERROR.
     * @return Command Status
     */
    public CommandStatus getRunStatus() {
        return runStatus;
    }

    /**
     * Gets the exception that was encountered
     * @return The exception that was encountered when running the command or null if there was no exception
     */
    public Throwable getError() {
        return error;
    }

    /**
     * Gets the debug buf in use
     * @return debug buf
     */
    public StringBuffer getDebugBuf() {
        return debugBuf;
    }

    /**
     * Sets a StringBuffer for debuging purposes. After that addDebug can be used to add
     * debug info.
     * @param pdebugBuf Debug Buffer to use
     */
    public void setDebugBuf(StringBuffer pdebugBuf) {
        debugBuf = pdebugBuf;
    }

}
