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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisMessageListener;
import org.aredis.cache.SubscriptionInfo;
import org.aredis.messaging.RedisSubscription;

public class TestRedisSubscription implements RedisMessageListener {

    private static class SubscriptionListener implements RedisMessageListener {
        private String subscribedChannelName;
        private volatile int hitCount;
        private volatile long lastHitTime;

        @Override
        public void receive(SubscriptionInfo s,
                String channelName, Object message) throws Exception {
            Map<String, SubscriptionListener> sm = currentSubscriptionMap;
            int pos = subscribedChannelName.indexOf('*');
            if(pos < 0) {
                if(!subscribedChannelName.equals(channelName)) {
                    System.out.println("INVALID channel " + channelName + " for subscribed channel " + subscribedChannelName);
                }
            }
            else {
                String prefix = subscribedChannelName;
                String suffix = "";
                if(pos >= 0) {
                    prefix = subscribedChannelName.substring(0, pos);
                    suffix = subscribedChannelName.substring(pos + 1);
                }
                if(!channelName.startsWith(prefix) || !channelName.endsWith(suffix)) {
                    System.out.println("INVALID channel " + channelName + " for PATTERN subscribed channel " + subscribedChannelName);
                }
            }
            String msg = (String) message;
            if(!channelName.substring(channelName.length() - 2).equals(msg.substring(msg.length() - 2))) {
                System.out.println("ERROR: Last 2 chars of message " + msg +" does not match with channel " + channelName);
            }
            if(sm.get(subscribedChannelName) == null) {
                System.out.println("ERROR: Got message for unsubscribed channel " + subscribedChannelName);
            }
            hitCount++;
            lastHitTime = System.currentTimeMillis();
        }
    }

    private static class SubscriptionCommand {
        private boolean isSubscription;
        private String channelName;
    }

    private static class SubscriptionThread extends Thread {
        private List<SubscriptionCommand> commands;
        private RedisSubscription subscription;

        @Override
        public void run() {
            for(SubscriptionCommand command : commands) {
                boolean isPattern = command.channelName.indexOf('*') >= 0;
                if(command.isSubscription) {
                    SubscriptionInfo subscriptionInfo = new SubscriptionInfo(command.channelName, currentSubscriptionMap.get(command.channelName), isPattern);
                    subscription.subscribe(subscriptionInfo);
                }
                else if("$uall".equals(command.channelName)) {
                    subscription.unsubscribeAll(false);
                }
                else if("$puall".equals(command.channelName)) {
                    subscription.unsubscribeAll(true);
                }
                else {
                    subscription.unsubscribe(command.channelName, isPattern);
                }
            }
        }
    }

    private static volatile Map<String, SubscriptionListener> currentSubscriptionMap = new HashMap<String, SubscriptionListener>();

    private RedisSubscription subscription;

    private static void changeSubscriptions(RedisSubscription subscription, String newSubscriptions[], Random r, int numThreads, int subscriptionsPerThread) throws InterruptedException {
        Map<String, SubscriptionListener> sm = currentSubscriptionMap;
        Map<String, SubscriptionListener> finalSm = new HashMap<String, SubscriptionListener>();
        int i, j, k;
        boolean isUnsubscribeAll = false;
        boolean isPUnsubscribeAll = false;
        for(String channelName : newSubscriptions) {
            if(Character.isDigit(channelName.charAt(0))) {
                String wildCard = "";
                String leadingZero = "";
                i = channelName.length() - 1;
                if(channelName.charAt(i) == '*') {
                    wildCard = "*";
                    channelName = channelName.substring(0, i);
                    i--;
                }
                if(i == 0) {
                    leadingZero = "0";
                }
                channelName = "channel" + wildCard + leadingZero + channelName;
            }
            if(channelName.equals("$uall")) {
                isUnsubscribeAll = true;
                continue;
            }
            if(channelName.equals("$puall")) {
                isPUnsubscribeAll = true;
                continue;
            }
            boolean isPattern = channelName.indexOf('*') >= 0;
            if(isPattern && isPUnsubscribeAll || !isPattern && isUnsubscribeAll) {
                continue;
            }
            SubscriptionListener listener = finalSm.get(channelName);
            if(listener != null) {
                continue; // Duplicate
            }
            listener = sm.get(channelName);
            if(listener == null) {
                listener = new SubscriptionListener();
                listener.subscribedChannelName = channelName;
            }
            finalSm.put(channelName, listener);
        }
        Map<String, SubscriptionListener> interimSm = new HashMap<String, SubscriptionListener>(sm);
        interimSm.putAll(finalSm);
        Map<String, Boolean> currentSubscriptionStatus = new HashMap<String, Boolean>();
        Map<String, Boolean> finalSubscriptionStatus = new HashMap<String, Boolean>();
        int totalChannels = interimSm.size();
        List<String> allChannels = new ArrayList<String>(totalChannels);
        allChannels.addAll(interimSm.keySet());
        for(String channelName : allChannels) {
            finalSubscriptionStatus.put(channelName, finalSm.containsKey(channelName));
            currentSubscriptionStatus.put(channelName, sm.containsKey(channelName));
        }
        List<SubscriptionCommand> lists[] = new List[numThreads];
        if(totalChannels > 0) {
            SubscriptionCommand nextCommand;
            Set<String> channelsForThread = new HashSet<String>();
            for(i = 0; i < lists.length; i++) {
                List<SubscriptionCommand> nextList = new ArrayList<SubscriptionCommand>(subscriptionsPerThread + 1);
                for(j = 0; j < subscriptionsPerThread;) {
                    boolean isSubscription = true;
                    int channelIndex = r.nextInt(2*totalChannels);
                    if(channelIndex >= totalChannels) {
                        isSubscription = false;
                        channelIndex -= totalChannels;
                    }
                    String channelName = allChannels.get(channelIndex);
                    nextCommand = new SubscriptionCommand();
                    nextCommand.channelName = channelName;
                    nextCommand.isSubscription = isSubscription;
                    nextList.add(nextCommand);
                    j++;
                    if(j < subscriptionsPerThread) {
                        int dupeStatus = r.nextInt(6);
                        if(dupeStatus < 4) {
                            isSubscription = true;
                            if(dupeStatus >= 2) {
                                isSubscription = false;
                                dupeStatus -= 2;
                            }
                            for(k = 0; k < dupeStatus; k++) {
                                nextCommand = new SubscriptionCommand();
                                nextCommand.channelName = channelName;
                                nextCommand.isSubscription = isSubscription;
                                nextList.add(nextCommand);
                                j++;
                                if(j >= subscriptionsPerThread) {
                                    break;
                                }
                            }
                        }
                    }
                }
                lists[i] = nextList;
                // After random stuff now make sure the thread leaves its channels in the
                // subscribed/unsubscribed state finally required
                boolean unsubscribeAllFirst = true;
                j = nextList.size();
                // First unsubscribe all in each thread if chosen by user. This is randomly done before or
                // after other channel unsubscribes (Pattern versus Non Pattern)
                if(isUnsubscribeAll || isPUnsubscribeAll) {
                    unsubscribeAllFirst = r.nextBoolean();
                    if(unsubscribeAllFirst) {
                        if(isUnsubscribeAll) {
                            nextCommand = new SubscriptionCommand();
                            nextCommand.channelName = "$uall";
                            nextList.add(nextCommand);
                        }
                        if(isPUnsubscribeAll) {
                            nextCommand = new SubscriptionCommand();
                            nextCommand.channelName = "$puall";
                            nextList.add(nextCommand);
                        }
                    }
                }
                channelsForThread.clear();
                for(; j > 0;) {
                    j--;
                    nextCommand = nextList.get(j);
                    String channelName = nextCommand.channelName;
                    boolean isPattern = channelName.indexOf('*') >= 0;
                    if(isPattern && isPUnsubscribeAll || !isPattern && isUnsubscribeAll) {
                        continue;
                    }
                    boolean subscriptionStatus = finalSubscriptionStatus.get(channelName);
                    if(!channelsForThread.contains(channelName)) {
                        channelsForThread.add(channelName);
                        currentSubscriptionStatus.put(channelName, subscriptionStatus);
                        if(subscriptionStatus != nextCommand.isSubscription) {
                            nextCommand = new SubscriptionCommand();
                            nextCommand.channelName = channelName;
                            nextCommand.isSubscription = subscriptionStatus;
                            nextList.add(nextCommand);
                        }
                    }
                }
                if(!unsubscribeAllFirst) {
                    if(isUnsubscribeAll) {
                        nextCommand = new SubscriptionCommand();
                        nextCommand.channelName = "$uall";
                        nextList.add(nextCommand);
                    }
                    if(isPUnsubscribeAll) {
                        nextCommand = new SubscriptionCommand();
                        nextCommand.channelName = "$puall";
                        nextList.add(nextCommand);
                    }
                }
            }
            // Fix all untouched channels by putting the command randomly in one of the lists
            for(String channelName : allChannels) {
                boolean isPattern = channelName.indexOf('*') >= 0;
                if(isPattern && isPUnsubscribeAll || !isPattern && isUnsubscribeAll) {
                    continue;
                }
                boolean subscriptionStatus = finalSubscriptionStatus.get(channelName);
                if(subscriptionStatus != currentSubscriptionStatus.get(channelName)) {
                    nextCommand = new SubscriptionCommand();
                    nextCommand.channelName = channelName;
                    nextCommand.isSubscription = subscriptionStatus;
                    i = r.nextInt(lists.length);
                    lists[i].add(nextCommand);
                }
            }
            // Set currentSubscriptionMap to interim to wait for unsubscriptions to happen
            currentSubscriptionMap = interimSm;
            // Start subscription/unsubscription threads
            SubscriptionThread threads[] = new SubscriptionThread[numThreads];
            for(i = 0; i < numThreads; i++) {
                threads[i] = new SubscriptionThread();
                threads[i].commands = lists[i];
                threads[i].subscription = subscription;
                threads[i].start();
            }
            for(i = 0; i < numThreads; i++) {
                threads[i].join();
            }
            for(SubscriptionListener listener : finalSm.values()) {
                listener.hitCount = 0;
                listener.lastHitTime = 0;
            }
            Thread.sleep(10000);
            // Set currentSubscriptionMap to final list
            currentSubscriptionMap = finalSm;
        }
    }

    private static void printSubscriptionStatus() {
        Map<String, SubscriptionListener> sm = currentSubscriptionMap;
        int inactiveCount = 0;
        Set<String> keySet = sm.keySet();
        for(String channelName : keySet) {
            SubscriptionListener listener = sm.get(channelName);
            if(listener.hitCount == 0) {
                inactiveCount++;
            }
        }
        boolean isFirst = true;
        System.out.println();
        if(inactiveCount < sm.size()) {
            System.out.println("CURRENTLY ACTIVE CHANNELS:");
            for(String channelName : keySet) {
                SubscriptionListener listener = sm.get(channelName);
                if(listener.hitCount > 0) {
                    if(!isFirst) {
                        System.out.println(',');
                    }
                    System.out.print(' ');
                    System.out.print(listener.subscribedChannelName);
                    System.out.print("(count: ");
                    System.out.println(listener.hitCount);
                    System.out.print("; ");
                    System.out.print(System.currentTimeMillis() - listener.lastHitTime);
                    System.out.print(" ms ago)");
                }
            }
            System.out.println();
        }
        else if(sm.size() == 0) {
            System.out.println("CURRENTLY NO ACTIVE CHANNELS:");
        }
        if(inactiveCount > 0) {
            System.out.println("WARNING: INACTIVE CHANNELS:");
            for(String channelName : keySet) {
                SubscriptionListener listener = sm.get(channelName);
                if(listener.hitCount == 0) {
                    if(!isFirst) {
                        System.out.println(',');
                    }
                    System.out.print(' ');
                    System.out.print(listener.subscribedChannelName);
                }
            }
            System.out.println();
        }
    }

    public static void main1(String [] args) throws Exception {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        String host = "localhost";
        RedisSubscription subscription = f.getSubscription(host, null);
        TestRedisSubscription listener = new TestRedisSubscription();
        listener.subscription = subscription;
        subscription.subscribe(new SubscriptionInfo("hello", listener));
        Thread.sleep(15000);
        subscription.unsubscribeAll(false);
        subscription.unsubscribeAll(false);
        subscription.subscribe(new SubscriptionInfo("hello1", listener));
        Thread.sleep(3600000);
    }

    public static void main(String args[]) throws Exception {
        AsyncRedisFactory f = new AsyncRedisFactory(null);
        String host = "localhost";
        RedisSubscription subscription = f.getSubscription(host, null);
        int numThreads = 10;
        int subscriptionsPerThread = 3;
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        String fromUser;
        Random r = new Random(10);
        while ((fromUser = stdIn.readLine()) != null && !"quit".equals(fromUser)) {
            printSubscriptionStatus();
            String [] channels = fromUser.split(" ");
            changeSubscriptions(subscription, channels, r, numThreads, subscriptionsPerThread);
        }
    }

    @Override
    public void receive(SubscriptionInfo subscriptionInfo, String channelName,
            Object message) throws Exception {
        System.out.println("Got message: " + message);
        if("done".equals(message)) {
            subscription.unsubscribe("hello", false);
        }
    }

}
