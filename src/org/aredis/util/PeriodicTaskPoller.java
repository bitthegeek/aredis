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

package org.aredis.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.cache.AsyncRedisFactory;
import org.aredis.cache.RedisSubscriptionConnection;

/**
 * A Singleton task poller which runs periodic tasks at a regular interval of 15 seconds. The registered periodic tasks
 * are run sequentially. It uses AsyncRedisConnection.getTimer to register itself with the timer when instantiated.
 * Since the tasks run sequentially in the thread of the timer they should be very light-weight like checking timeStamps
 * or statuses. Any other processing should be done asynchronously.
 *
 * @author Suresh
 *
 */
public class PeriodicTaskPoller implements Runnable {

    private static final Log log = LogFactory.getLog(RedisSubscriptionConnection.class);

    public static int POLL_INTERVAL_SECONDS = 15;

    private volatile List<PeriodicTask> tasks;

    private static volatile PeriodicTaskPoller instance;

    /**
     * Gets the single instance of the poller.
     *
     * @return The instance of PeriodicTaskPoller
     */
    public static PeriodicTaskPoller getInstance() {
        PeriodicTaskPoller p = instance;
        if(p == null) {
            synchronized(PeriodicTaskPoller.class) {
                p = instance;
                if(p == null) {
                    p = new PeriodicTaskPoller();
                    instance = p;
                }
            }
        }
        return p;
    }

    private PeriodicTaskPoller() {
        tasks = new ArrayList<PeriodicTask>();
        RedisTimer timer = AsyncRedisFactory.getTimer();
        long delayMillis = POLL_INTERVAL_SECONDS * 1000;
        timer.scheduleWithFixedDelay(this, delayMillis, delayMillis);
    }

    /**
     * Adds a task to be run at the specified interval of 15 seconds.
     * @param t Task
     * @return True if the task has been added, false if the task is already present
     */
    public synchronized boolean addTask(PeriodicTask t) {
        if(t == null) {
            throw new NullPointerException("Task to be added cannot be null");
        }
        List<PeriodicTask> taskList = tasks;
        boolean notExists = !taskList.contains(t);
        if(notExists) {
            taskList = new ArrayList<PeriodicTask>(taskList);
            taskList.add(t);
            tasks = taskList;
        }

        return notExists;
    }

    /**
     * Remove a task.
     * @param t Task to be removed
     * @return true if the task is removed or false if it not present
     */
    public synchronized boolean removeTask(PeriodicTask t) {
        if(t == null) {
            throw new NullPointerException("Task to be removed cannot be null");
        }
        List<PeriodicTask> taskList = tasks;
        int i, len = taskList.size();
        boolean found = false;
        for(i = 0; i < len; i++) {
            if(taskList.get(i) == t) {
                found = true;
                break;
            }
        }
        if(found) {
            taskList = new ArrayList<PeriodicTask>(taskList);
            taskList.remove(i);
            tasks = taskList;
        }
        return found;
    }

    /**
     * This method is public because it implements runnable. The runnable could have been an inner class.
     */
    @Override
    public void run() {
        long now = System.currentTimeMillis();
        List<PeriodicTask> taskList = tasks;
        for(PeriodicTask task : taskList) {
            try {
                task.doPeriodicTask(now);
            }
            catch(Exception e) {
                log.error("Error in Periodic Task", e);
            }
        }
    }
}
