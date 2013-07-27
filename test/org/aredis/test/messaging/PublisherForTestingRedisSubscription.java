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

package org.aredis.test.messaging;

import java.util.Arrays;
import java.util.Random;

import org.aredis.cache.AsyncRedisClient;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisCommand;
import org.aredis.cache.RedisCommandInfo;

public class PublisherForTestingRedisSubscription {
    public static void main(String args[]) throws InterruptedException {
        int i, numChannels = 10, sleepMillis = 50, unsentChannelCount, totalMessageCount= 0;
        String channelNames[] = new String[numChannels];
        String messages[] = new String[numChannels];
        Random r = new Random(10);
        for(i = 0; i < numChannels; i++) {
            String numStr = "" + (i / 10) + (i % 10);
            channelNames[i] = "channel" + numStr;
            messages[i] = "message" + numStr;
        }
        boolean messageSentFlags[] = new boolean[numChannels];
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        String host = "localhost";
        AsyncRedisClient con = f.getClient(host);
        int twoTimesNumChannels = 2 * numChannels;
        unsentChannelCount = numChannels;
        do {
            if(totalMessageCount >= twoTimesNumChannels && unsentChannelCount == 0) {
                Arrays.fill(messageSentFlags, false);
                totalMessageCount = 0;
                unsentChannelCount = numChannels;
                Thread.sleep(sleepMillis);
            }
            i = r.nextInt(numChannels);
            if(!messageSentFlags[i]) {
                messageSentFlags[i] = true;
                unsentChannelCount--;
            }
            totalMessageCount++;
            con.submitCommand(new RedisCommandInfo(RedisCommand.PUBLISH, channelNames[i], messages[i]), null);
        } while(true);
    }
}
