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

import java.util.concurrent.Executor;

import org.aredis.messaging.RedisSubscription;

/**
 * Subscription Info for a Redis Subscription including the Listener.
 * @author suresh
 *
 */
public class SubscriptionInfo {

    private String channelName;

    private boolean isPatternSubscription;

    private RedisMessageListener listener;

    private DataHandler dataHandler;

    private Object metaData;

    Executor executor;

    /**
     * Creates a SubscriptionInfo.
     * @param pchannelName channel name
     * @param plistener listener which is called when messages are received
     * @param pisPatternSubscription true to indicate a pattern subscription
     * @param pdataHandler Data Handler to de-serialize messages. If null OPTI_JAVA_HANDLER is used
     * @param pmetaData Meta Data if any for Data Handler or null
     * @param pexecutor Executor to use for calling the listener with messages. If null the one set in {@link RedisSubscription} is used
     */
    public SubscriptionInfo(String pchannelName, RedisMessageListener plistener, boolean pisPatternSubscription, DataHandler pdataHandler, Object pmetaData, Executor pexecutor) {
        if(pchannelName == null || (channelName = pchannelName.trim()).length() == 0) {
            throw new NullPointerException("Channel Name is Null or empty");
        }
        if(plistener == null) {
            throw new NullPointerException("Listener is Null");
        }
        listener = plistener;
        isPatternSubscription = pisPatternSubscription;
        dataHandler = pdataHandler;
        metaData = pmetaData;
        executor = pexecutor;
    }

    /**
     * Creates a SubscriptionInfo.
     * @param pchannelName channel name
     * @param plistener listener which is called when messages are received
     * @param pisPatternSubscription true to indicate a pattern subscription
     * @param pdataHandler Data Handler to de-serialize messages. If null OPTI_JAVA_HANDLER is used
     * @param pexecutor Executor to use for calling the listener with messages. If null the one set in {@link RedisSubscription} is used
     */
    public SubscriptionInfo(String pchannelName, RedisMessageListener plistener, boolean pisPatternSubscription, DataHandler pdataHandler, Executor pexecutor) {
        this(pchannelName, plistener, pisPatternSubscription, pdataHandler, null, pexecutor);
    }

    /**
     * Creates a SubscriptionInfo.
     * @param pchannelName channel name
     * @param plistener listener which is called when messages are received
     * @param pisPatternSubscription true to indicate a pattern subscription
     * @param pdataHandler Data Handler to de-serialize messages. If null OPTI_JAVA_HANDLER is used
     * @param pmetaData Meta Data if any for Data Handler or null
     */
    public SubscriptionInfo(String pchannelName, RedisMessageListener plistener, boolean pisPatternSubscription, DataHandler pdataHandler, Object pmetaData) {
        this(pchannelName, plistener, pisPatternSubscription, pdataHandler, pmetaData, null);
    }

    /**
     * Creates a SubscriptionInfo.
     * @param pchannelName channel name
     * @param plistener listener which is called when messages are received
     * @param pisPatternSubscription true to indicate a pattern subscription
     * @param pdataHandler Data Handler to de-serialize messages. If null OPTI_JAVA_HANDLER is used
     */
    public SubscriptionInfo(String pchannelName, RedisMessageListener plistener, boolean pisPatternSubscription, DataHandler pdataHandler) {
        this(pchannelName, plistener, pisPatternSubscription, pdataHandler, null, null);
    }

    /**
     * Creates a SubscriptionInfo.
     * @param pchannelName channel name
     * @param plistener listener which is called when messages are received
     * @param pisPatternSubscription true to indicate a pattern subscription
     */
    public SubscriptionInfo(String pchannelName, RedisMessageListener plistener, boolean pisPatternSubscription) {
        this(pchannelName, plistener, pisPatternSubscription, null, null, null);
    }

    /**
     * Creates a SubscriptionInfo.
     * @param pchannelName channel name or regular subscription
     * @param plistener listener which is called when messages are received
     */
    public SubscriptionInfo(String pchannelName, RedisMessageListener plistener) {
        this(pchannelName, plistener, false, null, null, null);
    }

    /**
     * Gets the channel name.
     * @return channel name
     */
    public String getChannelName() {
        return channelName;
    }

    /**
     * Gets the listener.
     * @return listener which is called when messages are received
     */
    public RedisMessageListener getListener() {
        return listener;
    }

    /**
     * Checks if this is a pattern subscription.
     * @return true to indicate a pattern subscription
     */
    public boolean isPatternSubscription() {
        return isPatternSubscription;
    }

    /**
     * Gets the Data Handler.
     * @return Data Handler to de-serialize messages. null if none is set
     */
    public DataHandler getDataHandler() {
        return dataHandler;
    }

    /**
     * Gets the executor.
     * @return Executor to use for calling the listener with messages. If null the one set in {@link RedisSubscription} is used
     */
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Gets the meta data.
     * @return meta data that is used along with the Data Handler to de-serialize messages.
     */
    public Object getMetaData() {
        return metaData;
    }

}
