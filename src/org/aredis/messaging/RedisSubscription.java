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

package org.aredis.messaging;

import java.util.concurrent.Executor;

import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;
import org.aredis.cache.RedisSubscriptionConnection;
import org.aredis.cache.SubscriptionInfo;
import org.aredis.net.AsyncSocketTransport;

/**
 * Used to create a subscription connection to a Redis Server after which you can subscribe to channels.
 * @author suresh
 *
 */
public class RedisSubscription extends RedisSubscriptionConnection {

    /**
     * Creates a RedisSubscription. The preferred way to create a RedisSubscription is via {@link AsyncRedisFactory}.
     * @param pcon Async Socket Transport for the connection
     * @param pdbIndex dbIndex of the connection
     * @param ptaskExecutor Executor to use for sending the messages to the listeners. It is strongly recommended to pass a non null value.
     * If null an internal Thread Pool will be used to make the calls to the listeners.
     */
    public RedisSubscription(AsyncSocketTransport pcon, int pdbIndex,
            Executor ptaskExecutor) {
        super(pcon, pdbIndex, ptaskExecutor);
    }

    /**
     * Subscribe to a channel or to a channel pattern. The subscription happens asynchronously. If the server is down the subscription happens upon
     * re-connection when the server comes back up.
     * @param subscriptionInfo subscription info
     */
    public void subscribe(SubscriptionInfo subscriptionInfo) {
        RedisCommand command = RedisCommand.SUBSCRIBE;
        if(subscriptionInfo.isPatternSubscription()) {
            command = RedisCommand.PSUBSCRIBE;
        }
        RedisCommandInfo commandInfo = new RedisCommandInfo(command, subscriptionInfo.getChannelName());
        submitCommand(commandInfo, subscriptionInfo);
    }

    /**
     * Unsubscribe to a channel or from a channel pattern. The unsubscription happens asynchronously. If the server is down the unsubscription happens upon
     * re-connection when the server comes back up.
     * @param channelName channel name
     * @param isPatternSubscription true for a pattern unsubscription false otherwise
     */
    public void unsubscribe(String channelName, boolean isPatternSubscription) {
        if(channelName == null || channelName.trim().length() == 0) {
            throw new NullPointerException("Channel to Unsubscribe is null or empty");
        }
        RedisCommand command = RedisCommand.UNSUBSCRIBE;
        if(isPatternSubscription) {
            command = RedisCommand.PUNSUBSCRIBE;
        }
        RedisCommandInfo commandInfo = new RedisCommandInfo(command, channelName);
        submitCommand(commandInfo, null);
    }

    /**
     * Unsubscribe from all channels or from all channel patterns. The unsubscription happens asynchronously. If the server is down the unsubscription happens upon
     * re-connection when the server comes back up.
     * @param isPatternSubscription true for a pattern unsubscription false otherwise
     */
    public void unsubscribeAll(boolean isPatternSubscription) {
        RedisCommand command = RedisCommand.UNSUBSCRIBE;
        if(isPatternSubscription) {
            command = RedisCommand.PUNSUBSCRIBE;
        }
        RedisCommandInfo commandInfo = new RedisCommandInfo(command);
        submitCommand(commandInfo, null);
    }
}
