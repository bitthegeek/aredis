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

import java.util.concurrent.Future;

/**
 * Specifies the different methods to run Redis Commands.
 * @author Suresh
 *
 */
public interface AsyncRedisClient {
    /**
     * Submit multiple Redis Commands.
     * @param commands commands to run along with their parameters
     * @param completionHandler Handler to call upon command completion. Can be null
     * @param requireFutureResult Whether a Future return value is required
     * @param isSyncCallback If true the callback is made directly by the Socket Channel Thread and not the configured executor. Pass true only if the handler logic is extremely light like moving the command to another Q.
     * @return a Future Object to retrieve the submitted commands containing the results or null if requireFutureResult is passed as false
     */
    Future<RedisCommandInfo[]> submitCommands(RedisCommandInfo commands[], AsyncHandler<RedisCommandInfo[]> completionHandler, boolean requireFutureResult, boolean isSyncCallback);

    /**
     * Submit a Redis Command.
     * @param command command to run along with its parameters
     * @param completionHandler Handler to call upon command completion. Can be null
     * @param requireFutureResult Whether a Future return value is required
     * @param isSyncCallback If true the callback is made directly by the Socket Channel Thread and not the configured executor. Pass true only if the handler logic is extremely light like moving the command to another Q.
     * @return a Future Object to retrieve the submitted command containing the result or null if requireFutureResult is passed as false
     */
    Future<RedisCommandInfo> submitCommand(RedisCommandInfo command, AsyncHandler<RedisCommandInfo> completionHandler, boolean requireFutureResult, boolean isSyncCallback);

    /**
     * Submit multiple Redis Commands.
     * @param commands commands to run along with their parameters
     * @return a Future Object to retrieve the submitted commands containing the results
     */
    Future<RedisCommandInfo[]> submitCommands(RedisCommandInfo commands[]);

    /**
     * Submit multiple Redis Commands.
     * @param commands commands to run along with their parameters
     * @param completionHandler Handler to call upon command completion. Can be null if you want to send the commands and forget.
     */
    void submitCommands(RedisCommandInfo commands[],
            AsyncHandler<RedisCommandInfo[]> completionHandler);

    /**
     * Submit a Redis Command.
     * @param command command to run along with its parameters
     * @return a Future Object to retrieve the submitted command containing the result
     */
    Future<RedisCommandInfo> submitCommand(RedisCommandInfo command);

    /**
     * Submit a Redis Command.
     * @param command command to run along with its parameters
     * @param completionHandler Handler to call upon command completion. Can be null if you want to send the command and forget.
     */
    void submitCommand(RedisCommandInfo command,
            AsyncHandler<RedisCommandInfo> completionHandler);

    /**
     * Submit a Redis Command. The default DataHandler is used.
     * @param command command to run
     * @param params command parameters
     * @return a Future Object to retrieve the submitted command containing the result
     */
    Future<RedisCommandInfo> submitCommand(RedisCommand command, Object ... params);

    /**
     * Submit a Redis Command. The default DataHandler is used.
     * @param metaData Metadata to be used if the DataHandler requires metadata like the Object class
     * @param command command to run
     * @param params command parameters
     * @return a Future Object to retrieve the submitted command containing the result
     */
    Future<RedisCommandInfo> submitCommand(Object metaData, RedisCommand command, Object ... params);

    /**
     * Submit a Redis Command.
     * @param dataHandler Data Handler to be used. Pass null to use the default Data Handler
     * @param command command to run
     * @param params command parameters
     * @return a Future Object to retrieve the submitted command containing the result
     */
    Future<RedisCommandInfo> submitCommand(DataHandler dataHandler, RedisCommand command, Object ... params);

    /**
     * Submit a Redis Command.
     * @param dataHandler Data Handler to be used. Pass null to use the default Data Handler
     * @param metaData Metadata to be used if the DataHandler requires metadata like the Object class
     * @param command command to run
     * @param params command parameters
     * @return a Future Object to retrieve the submitted command containing the result
     */
    Future<RedisCommandInfo> submitCommand(DataHandler dataHandler, Object metaData, RedisCommand command, Object ... params);

    /**
     * Send a command without looking for a return value. The default DataHandler is used.
     * @param command command to run
     * @param params command parameters
     */
    void sendCommand(RedisCommand command, Object ... params);

    /**
     * Send a command without looking for a return value. The default DataHandler is used.
     * @param metaData Metadata to be used if the DataHandler requires metadata like the Object class
     * @param command command to run
     * @param params command parameters
     */
    void sendCommand(Object metaData, RedisCommand command, Object ... params);

    /**
     * Send a command without looking for a return value.
     * @param dataHandler Data Handler to be used. Pass null to use the default Data Handler
     * @param command command to run
     * @param params command parameters
     */
    void sendCommand(DataHandler dataHandler, RedisCommand command, Object ... params);

    /**
     * Send a command without looking for a return value.
     * @param metaData Metadata to be used if the DataHandler requires metadata like the Object class
     * @param command command to run
     * @param params command parameters
     */
    void sendCommand(DataHandler dataHandler, Object metaData, RedisCommand command, Object ... params);

    /**
     * Submit a Redis Command. The default DataHandler is used.
     * @param completionHandler Handler to call upon command completion.
     * @param command command to run
     * @param params command parameters
     */
    void submitCommand(AsyncHandler<RedisCommandInfo> completionHandler, RedisCommand command, Object ... params);

    /**
     * Submit a Redis Command. The default DataHandler is used.
     * @param completionHandler Handler to call upon command completion.
     * @param metaData Metadata to be used if the DataHandler requires metadata like the Object class
     * @param command command to run
     * @param params command parameters
     */
    void submitCommand(AsyncHandler<RedisCommandInfo> completionHandler, Object metaData, RedisCommand command, Object ... params);

    /**
     * Submit a Redis Command.
     * @param completionHandler Handler to call upon command completion.
     * @param dataHandler Data Handler to be used. Pass null to use the default Data Handler
     * @param command command to run
     * @param params command parameters
     */
    void submitCommand(AsyncHandler<RedisCommandInfo> completionHandler, DataHandler dataHandler, RedisCommand command, Object ... params);

    /**
     * Submit a Redis Command.
     * @param completionHandler Handler to call upon command completion.
     * @param dataHandler Data Handler to be used. Pass null to use the default Data Handler
     * @param metaData Metadata to be used if the DataHandler requires metadata like the Object class
     * @param command command to run
     * @param params command parameters
     */
    void submitCommand(AsyncHandler<RedisCommandInfo> completionHandler, DataHandler dataHandler, Object metaData, RedisCommand command, Object ... params);

}
