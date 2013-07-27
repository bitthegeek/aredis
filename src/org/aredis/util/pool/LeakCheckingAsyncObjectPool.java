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

package org.aredis.util.pool;

import java.util.Iterator;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.util.PeriodicTask;
import org.aredis.util.PeriodicTaskPoller;

/**
 * A Object Pool which prints an error log message when a object leak is detected.
 * @author Suresh
 *
 * @param <E> Type of Member Object
 */
public class LeakCheckingAsyncObjectPool<E> extends AsyncObjectPool<E> {

    private static final Log log = LogFactory.getLog(LeakCheckingAsyncObjectPool.class);

    private class LeakCheckingTask implements PeriodicTask {

        @Override
        public void doPeriodicTask(long now) {
            Iterator<PoolMemberHolder<E>> it = memberIterator();
            while(it.hasNext()) {
                PoolMemberHolder<E> memberHolder = it.next();
                long holdTimeMillis;
                if(memberHolder.isInUse() && (holdTimeMillis = now - memberHolder.getLastBorrowTime()) > maxHoldTimeMillis) {
                    if(now - memberHolder.lastErrorLogTime >= leakLogIntervalMillis) {
                        memberHolder.lastErrorLogTime = now;
                        log.error("AsyncObjectPool LEAK ALERT: Connection/Object " + memberHolder.getMember() + " borrowed " + (holdTimeMillis/1000) + " seconds ago is not yet returned. This will exhaust the pool. Please fix the bug causing the leak.");
                    }
                }
            }
        }

    }

    /**
     * DEFAULT hold time in milliseconds.
     */
    public static long DEFAULT_MAX_HOLD_TIME_MILLIS = 30000; // 30 seconds

    /**
     * Default leak log interval in milliseconds.
     */
    public static long DEFAULT_LEAK_LOG_INTERVAL = 600000; // Every 10 mins

    /**
     * Max hold time.
     */
    protected long maxHoldTimeMillis;

    /**
     * Leak log interval.
     */
    protected long leakLogIntervalMillis;

    private LeakCheckingTask leakCheckingTask;

    /**
     * Creates a Leak checking AsyncObject Pool with default parameters for max hold time and log intervals.
     * @param pobjectManager Object Manager to Create and Destroy Pool member objects
     * @param ppoolSize Pool Size
     * @param ptaskExecutor Executor to use. Mandatory in case of async usage, not needed otherwise
     */
    public LeakCheckingAsyncObjectPool(
            AsyncPoolMemberManager<E> pobjectManager, int ppoolSize,
            Executor ptaskExecutor) {
        super(pobjectManager, ppoolSize, ptaskExecutor);
        maxHoldTimeMillis = DEFAULT_MAX_HOLD_TIME_MILLIS;
        leakLogIntervalMillis = DEFAULT_LEAK_LOG_INTERVAL;
        leakCheckingTask = new LeakCheckingTask();
        PeriodicTaskPoller periodicTaskPoller = PeriodicTaskPoller.getInstance();
        periodicTaskPoller.addTask(leakCheckingTask);
    }

    /**
     * Gets max hold time.
     * @return Max hold time
     */
    public long getMaxHoldTimeMillis() {
        return maxHoldTimeMillis;
    }

    /**
     * Sets the max hold time. If a borrowed object is not returned within this time an error message is printed.
     * @param pmaxHoldTimeMillis Max Hold Time
     */
    public void setMaxHoldTimeMillis(long pmaxHoldTimeMillis) {
        maxHoldTimeMillis = pmaxHoldTimeMillis;
    }

    /**
     * Gets the leak log interval.
     * @return Leak log interval
     */
    public long getLeakLogIntervalMillis() {
        return leakLogIntervalMillis;
    }

    /**
     * Sets the leak log interval. This is the interval in which successive Log messages are printed for this pool.
     * @param pleakLogIntervalMillis Leak Log interval
     */
    public void setLeakLogIntervalMillis(long pleakLogIntervalMillis) {
        leakLogIntervalMillis = pleakLogIntervalMillis;
    }

}
