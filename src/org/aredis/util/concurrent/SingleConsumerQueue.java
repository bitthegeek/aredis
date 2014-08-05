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

package org.aredis.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.aredis.cache.AsyncRedisConnection;

/**
 * <p>
 * This is a Wrapper over a {@link LinkedBlockingQueue} for use with a Single Consumer. This is used by the internal request and
 * response Q's of {@link AsyncRedisConnection}. The Consumer Thread can optionally quit on an empty Q in which case
 * the Q is marked as IDLE. The writer has an option to acquire the Q if it is IDLE when writing to the head of the Q.
 * If the Q is acquired then then the writer should initiate the listener.
 * </p>
 *
 * <p>
 * Note: Only One consumer should be receiving for a SingleConsumerQueue. This can be ensured by using add
 * method to acquire an idle consumer status and initiate the consumer and the remove method to set back the consumer
 * status to idle and quit the consumer thread in case of an empty Q.
 * </p>
 *
 * @author Suresh
 *
 * @param <E> Type of items in the Q
 */
public class SingleConsumerQueue<E> {

    /**
     * Callback Interface to get called back when the IDLE status of the Q changes.
     * @author Suresh
     *
     * @param <E>
     */
    public static interface IdleListener<E> {
        /**
         * Gets called before the Q is marked idle when releasing a Q. The call is from the consumer thread.
         * @param q The Q
         */
        void beforeIdle(SingleConsumerQueue<E> q);

        /**
         * Gets called after the Q is marked active when acquiring an idle Q. The call is from the writer thread.
         * @param q The Q
         */
        void afterAcquireIdle(SingleConsumerQueue<E> q);
    }

    public static enum QueueStatus{ACTIVE, IDLE};

    /**
     * Default value of maxEnqueTimeMillis.
     */
    public static final int DEFAULT_MAX_ENQUE_TIME_MILLIS = 10;

    private static final int FETCH_SIZE = 256;

    private BlockingQueue<E> q;

    private int lindex;

    private List<E> l;

    private volatile QueueStatus status;

    private IdleListener<E> idleListener;

    private int capacity;

    private long maxEnqueTimeMillis;

    /**
     * Creates a Q with the given capacity and maxEnqueTimeMillis to wait for when adding to the Q in case the Q is
     * full.
     * @param pcapacity Q capacity or 0 for unbounded
     * @param pmaxEnqueTimeMillis Max Milliseconds to wait when adding to Q if the Q is full
     */
    public SingleConsumerQueue(int pcapacity, long pmaxEnqueTimeMillis) {
        if(pcapacity < 0) {
            throw new IllegalArgumentException("Q capacity cannot be negative");
        }
        if(pmaxEnqueTimeMillis < 0) {
            throw new IllegalArgumentException("Max Enque time cannot be negative");
        }
        q = new LinkedBlockingQueue<E>();
        l = new ArrayList<E>(FETCH_SIZE);
        status = QueueStatus.IDLE;
        capacity = pcapacity;
        maxEnqueTimeMillis = pmaxEnqueTimeMillis;
    }

    /**
     * Creates a Q with the given capacity and default maxEnqueTimeMillis of 5 seconds
     * @param pcapacity Q capacity or 0 for unbounded
     */
    public SingleConsumerQueue(int pcapacity) {
        this(pcapacity, DEFAULT_MAX_ENQUE_TIME_MILLIS);
    }

    /**
     * Creates a Q with unbounded capacity.
     */
    public SingleConsumerQueue() {
        this(0, DEFAULT_MAX_ENQUE_TIME_MILLIS);
    }

    /**
     * Adds an item to the head of the Q.
     * @param e item to add
     * @param acquireIfIdle pass true to acquire the Q if currently idle. When used concurrently exactly one of the
     * callers gets the return value as true.
     * @return true if an idle Q has been acquired and marked active in which case the caller should start the Consumer
     * thread. Always returs false if acquireIfIdle has been passed as false.
     */
    public boolean add(E e, boolean acquireIfIdle) {
        String msg = null;
        if(capacity == 0 || maxEnqueTimeMillis == 0) {
            if (!q.offer(e)) {
                msg = "Queue Full";
            }
        }
        else {
            try {
                if(!q.offer(e, maxEnqueTimeMillis, TimeUnit.MILLISECONDS)) {
                    msg = "Queue Full after " + maxEnqueTimeMillis + " ms";
                }
            } catch (InterruptedException e1) {
                msg = "add Operation Interrupted";
            }
        }
        if(msg != null) {
            throw new IllegalStateException(msg);
        }

        return acquireIfIdle && acquireIdle();
    }

    /**
     * Remove from the tail of the Q.
     * @param timeoutMicros Max microseconds to wait for in case the Q is empty
     * @param releaseIfEmpty If true then the Q is marked idle if the Q is empty and this method returns null.
     * @return Item or null if the Q is empty
     */
    public E remove(long timeoutMicros, boolean releaseIfEmpty) {
        int len = l.size();
        E e = null;
        if(lindex >= len) {
            l.clear();
            q.drainTo(l, FETCH_SIZE);
            len = l.size();
            lindex = 0;
            if(len == 0 && timeoutMicros > 0) {
                try {
                    e = q.poll(timeoutMicros, TimeUnit.MICROSECONDS);
                } catch (InterruptedException ie) {
                }
            }
        }

        if(len > 0) {
            e = l.get(lindex);
            lindex++;
        }

        if(releaseIfEmpty && e == null) {
            boolean ensuredEmptyOrAvail;
            do {
                ensuredEmptyOrAvail = true;
                IdleListener<E> listener = idleListener;
                if(listener != null) {
                    listener.beforeIdle(this);
                }
                status = QueueStatus.IDLE;
                if(q.size() > 0 && acquireIdle()) {
                    e = remove(0, false);
                    // In the previous code the e == null was considered impossible and threw
                    // an IllegalStateException. However it is possible if after the size check
                    // and before acquiring the idle Q another thread acquired dequeued 1 item
                    // successfully and then released it on attempting another remove in which
                    // case acquireIdle will succeed but the Q will be empty
                    if(e == null) {
                        ensuredEmptyOrAvail = false;
                    }
                }
            } while (!ensuredEmptyOrAvail);
        }

        return e;
    }

    /**
     * Push to head of Q. Non concurrent method to be used only by consumer.
     * @param e Item
     */
    public void push(E e) {
        if(lindex > 0) {
            lindex--;
            l.set(lindex, e);
        }
        else {
            l.add(lindex, e);
        }
    }

    /**
     * Unconditionally mark the Q idle. The caller should be holding the Q. Normally there should
     * be a double check after this call to ensure that the idle condition continues to hold which
     * usually is that the Q continues to be empty. If not the processing action should be initiated
     * by trying to acquire back the idle Q.
     */
    public void markIdle() {
        IdleListener<E> listener = idleListener;
        if(listener != null) {
            listener.beforeIdle(this);
        }
        status = QueueStatus.IDLE;
    }

    /**
     * Acquires an idle Q and marks it as active.
     * @return true if the Q is idle and has been acquired by this thread
     */
    public boolean acquireIdle() {
        boolean acquired = false;
        if(status == QueueStatus.IDLE) {
            synchronized(this) {
                if(status == QueueStatus.IDLE) {
                    status = QueueStatus.ACTIVE;
                    acquired = true;
                }
            }
        }

        if(acquired) {
            IdleListener<E> listener = idleListener;
            if(listener != null) {
                listener.afterAcquireIdle(this);
            }
        }

        return acquired;
    }

    /**
     * Gets the size of the Q.
     * @return Q size
     */
    public int size() {
        return q.size() + l.size();
    }

    /**
     * Gets the idle listener in use.
     * @return The idle listener or null if not set
     */
    public IdleListener<E> getIdleListener() {
        return idleListener;
    }

    /**
     * Sets an idle listener which is called whenever the Q status changes from IDLE to ACTIVE or vice-versa.
     * @param pidleListener Idle listener or null to unset the current listener
     */
    public void setIdleListener(IdleListener<E> pidleListener) {
        idleListener = pidleListener;
    }
}
