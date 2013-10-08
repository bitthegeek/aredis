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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.cache.AsyncRedisConnection;
import org.aredis.util.ArrayWrappingList;
import org.aredis.util.concurrent.SynchronizedStack;

/**
 * An fixed size Object pool with the feature of Async borrow used as connection pool by aredis for MULTI-EXEC transactions with WATCH commands.
 * The pool assumes the object to be self managed. So it does not do things like validation or re-connections. It works by using a stack and a Q.
 * The stack is used to store free objects and the Q for waiting users. Ideally one of the stack or Q are empty. A stack is used for the Objects
 * so that the Most Recently used object is again re-used. This ensures that idle Objects can be timed out as it is done in {@link AsyncRedisConnection}.
 * @author suresh
 *
 * @param <E>
 */
public class AsyncObjectPool<E> {

    /**
     * Class Holding pool members.
     * @author suresh
     *
     * @param <T>
     */
    protected static class PoolMemberHolder<T> implements Comparable<PoolMemberHolder<T>> {
        private T member;

        private volatile long lastBorrowTime;

        private volatile boolean isInUse;

        private volatile int removalStatus;

        protected long lastErrorLogTime;

        private void changeInUseStatus(boolean pisInUse) {
            isInUse = pisInUse;
            if(pisInUse) {
                lastBorrowTime = System.currentTimeMillis();
            }
        }

        /**
         * Identity System.identityHashCode base comparator on members so that Binary Search can be used to locate a member.
         */
        @Override
        public int compareTo(PoolMemberHolder<T> o) {
            int result = 1;
            if(o != null) {
                result = System.identityHashCode(member) - System.identityHashCode(o.member);
            }

            return result;
        }

        /**
         * Gets the member.
         * @return member
         */
        public T getMember() {
            return member;
        }

        /**
         * Gets the last borrow time irrespective of whether it is currently in use or not
         * @return Last borrow time
         */
        public long getLastBorrowTime() {
            return lastBorrowTime;
        }

        /**
         * Checks if pool member is currently in used (Borrowed)
         * @return true if member is in use
         */
        public boolean isInUse() {
            return isInUse;
        }

        /**
         * Returns the removal status in case the pool size is reduced using setPoolSize. The int constant AVAIBABLE is the expected value.
         * Other values like MARKED and REMOVED indicate that the member is to be removed and is for internal use.
         * @return Removal Status
         */
        public int getRemovalStatus() {
            return removalStatus;
        }
    }

    private static class PoolUser<T> implements Runnable, Callable<T> {
        private AvailabeCallback<T> callback;

        private AsyncPoolMemberManager<T> objectManager;

        private FutureTask<T> futureAvailability;

        private T result;

        private boolean isSyncWaiting = true;;

        private long futureExpiryTime;

        public PoolUser(AsyncPoolMemberManager<T> pobjectManager) {
            objectManager = pobjectManager;
        }

        @Override
        public void run() {
            try {
                try {
                    objectManager.onBorrow(result);
                }
                catch(Exception e) {
                    log.error("Error in onBorrow callback. Ignoring", e);
                }
                callback.use(result);
            } catch (Exception e) {
                log.error("Error during Pool Availability callback", e);
            }
        }

        @Override
        public T call() throws Exception {
            return result;
        }

        public boolean assignObject(PoolMemberHolder<T> holder, Executor taskExecutor) {
            boolean success = false;
            if(callback != null) {
                holder.changeInUseStatus(true);
                success = true;
                result = holder.member;
                taskExecutor.execute(this);
            }
            else if(futureAvailability != null) {
                if(futureExpiryTime <= 0 || System.currentTimeMillis() <= futureExpiryTime) {
                    holder.changeInUseStatus(true);
                    success = true;
                    result = holder.member;
                    try {
                        objectManager.onBorrow(result);
                    }
                    catch(Exception e) {
                        log.error("Error in onBorrow callback. Ignoring", e);
                    }
                    futureAvailability.run();
                }
            }
            else {
                synchronized(this) {
                    if(isSyncWaiting) {
                        holder.changeInUseStatus(true);
                        success = true;
                        result = holder.member;
                        notifyAll();
                    }
                }
            }
            return success;
        }

        public synchronized T syncWait(long timeoutMillis) {
            try {
                if(result == null) {
                    if(timeoutMillis > 0) {
                        wait(timeoutMillis);
                    }
                    else {
                        wait();
                    }
                }
            } catch (InterruptedException e) {
                log.error("syncWait Interrupted", e);
            }
            finally {
                isSyncWaiting = false;
            }

            return result;
        }
    }

    private static final Log log = LogFactory.getLog(AsyncObjectPool.class);

    private static final int AVAILABLE = 0;

    private static final int MARKED = 1;

    private static final int REMOVED = 2;

    private AsyncPoolMemberManager<E> objectManager;

    private volatile int poolSize;

    private volatile PoolMemberHolder<E>[] poolMembers;

    private volatile List<PoolMemberHolder<E>> poolMemberList;

    private SynchronizedStack<PoolMemberHolder<E>> freeMembers;

    Queue<PoolUser<E>> waitingUsers;

    private Executor taskExecutor;

    /**
     * This is only intended for debugging purposes to measure how many times the
     * relatively expensive call to ensureEmptyWaitersOrMembers is made. To track
     * Assign this to an AtomicInteger.
     */
    public AtomicInteger reconcileCount;

    /**
     * Creates an AsyncObject Pool.
     * @param pobjectManager Object Manager to Create and Destroy Pool member objects
     * @param ppoolSize Pool Size
     * @param ptaskExecutor Executor to use. Mandatory in case of async usage, not needed otherwise
     */
    public AsyncObjectPool(AsyncPoolMemberManager<E> pobjectManager, int ppoolSize, Executor ptaskExecutor) {
        if(ppoolSize < 1 || ppoolSize > 1000) {
            throw new IllegalArgumentException("Illegal value of poolSize: " + ppoolSize + " should be 1-1000");
        }
        objectManager = pobjectManager;
        taskExecutor = ptaskExecutor;
        poolSize = ppoolSize;
        poolMembers = new PoolMemberHolder[ppoolSize];
        poolMemberList = new ArrayWrappingList<AsyncObjectPool.PoolMemberHolder<E>>(poolMembers);
        freeMembers = new SynchronizedStack<>(poolSize);
        waitingUsers = new LinkedBlockingQueue<PoolUser<E>>();
        int i;
        for(i = 0; i < poolMembers.length; i++) {
            PoolMemberHolder<E> holder = new PoolMemberHolder<E>();
            holder.member = objectManager.createObject();
            poolMembers[i] = holder;
        }
        Arrays.sort(poolMembers);
        for(i = 0; i < poolMembers.length; i++) {
            freeMembers.offerFirst(poolMembers[i]);
        }
    }

    private int findMember(PoolMemberHolder<E> pms[], E member) {
        int memberCode = System.identityHashCode(member);
        int len = pms.length;
        int low = 0;
        int high = len - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = System.identityHashCode(pms[mid].member);

            if (midVal < memberCode) {
                low = mid + 1;
            }
            else {
                high = mid - 1;
            }
        }
        E testMember;
        int index = -(low + 1);  // key not found.
        while(low < len && memberCode == System.identityHashCode(testMember = pms[low].member)) {
            if(member == testMember) {
                index = low;
                break;
            }
            low++;
        }

        return index;
    }

    private synchronized void ensureEmptyWaitersOrMembers() {
        boolean tryAgain = false;
        do {
            tryAgain = false;
            PoolMemberHolder<E> holder = freeMembers.pollFirst();
            if(holder != null) {
                PoolUser<E> poolUser;
                while((poolUser = waitingUsers.poll()) != null) {
                    if(poolUser.assignObject(holder, taskExecutor)) {
                        tryAgain = true;
                        break;
                    }
                }
                if(poolUser == null) {
                    freeMembers.offerFirst(holder);
                }
            }
            if(reconcileCount != null) {
                reconcileCount.incrementAndGet();
            }
        } while(tryAgain || freeMembers.size() > 0 && waitingUsers.size() > 0);
    }

    /**
     * Synchronously borrow from the pool.
     * @param timeoutMillis Max time to wait
     * @return member Object from pool or null if timeout has expired
     */
    public E syncBorrow(long timeoutMillis) {
        E member = null;
        PoolMemberHolder<E> memberHolder = freeMembers.pollFirst();
        if(memberHolder != null) {
            memberHolder.changeInUseStatus(true);
            member = memberHolder.member;
        }
        else if(timeoutMillis >= 0) {
            PoolUser<E> poolUser = new PoolUser<E>(objectManager);
            waitingUsers.add(poolUser);
            if(freeMembers.size() > 0) {
                ensureEmptyWaitersOrMembers();
            }
            member = poolUser.syncWait(timeoutMillis);
        }

        if(member != null) {
            try {
                objectManager.onBorrow(member);
            }
            catch(Exception e) {
                log.error("Error in onBorrow callback. Ignoring", e);
            }
        }

        return member;
    }

    /**
     * Borrow returning a Future
     * @param timeoutMillis Max time intended to wait. The actual timeout on the get call on the future object should be
     * at least 10 ms greater than this value. Otherwise the member Object would returning after the timeout passed to get will
     * be considered as borrowed by the pool leading to a leak. The get method on the future may return a null if this timeout is past.
     * @return Future result for the borrowed Object
     */
    public Future<E> borrow(long timeoutMillis) {
        PoolUser<E> poolUser = new PoolUser<E>(objectManager);
        FutureTask<E> result = new FutureTask<E>(poolUser);
        PoolMemberHolder<E> memberHolder = freeMembers.pollFirst();
        if(memberHolder != null) {
            memberHolder.changeInUseStatus(true);
            poolUser.result = memberHolder.member;
            try {
                objectManager.onBorrow(memberHolder.member);
            }
            catch(Exception e) {
                log.error("Error in onBorrow callback. Ignoring", e);
            }
            result.run();
        }
        else if(timeoutMillis >= 0) {
            if(timeoutMillis > 0) {
                poolUser.futureExpiryTime = System.currentTimeMillis() + timeoutMillis;
            }
            poolUser.futureAvailability = result;
            waitingUsers.add(poolUser);
            if(freeMembers.size() > 0) {
                ensureEmptyWaitersOrMembers();
            }
        }

        return result;
    }

    /**
     * Asynchronous borrow. Note that if a member Object is immediately available in pool it will be returned and the
     * callback will not be made.
     * @param callback Callback which will be invoked in case with an available member in case no pool member is
     * currently free
     * @return member Object if immediately available or null if not
     */
    public E asyncBorrow(AvailabeCallback<E> callback) {
        if(taskExecutor == null) {
            throw new IllegalStateException("AsyncObjectPool.taskExecutor cannot be null for Async borrow");
        }
        E member = null;
        PoolMemberHolder<E> memberHolder = freeMembers.pollFirst();
        if(memberHolder != null) {
            memberHolder.changeInUseStatus(true);
            member = memberHolder.member;
            try {
                objectManager.onBorrow(member);
            }
            catch(Exception e) {
                log.error("Error in onBorrow callback. Ignoring", e);
            }
        }
        else {
            PoolUser<E> poolUser = new PoolUser<E>(objectManager);
            poolUser.callback = callback;
            waitingUsers.add(poolUser);
            if(freeMembers.size() > 0) {
                ensureEmptyWaitersOrMembers();
            }
        }

        return member;
    }

    private synchronized void updateStackCapacity() {
        if(poolMembers.length == poolSize) {
            if(!freeMembers.setCapacity(poolSize)) {
                throw new RuntimeException("Unexpected error in updating the freeMembers stack capacity from " + freeMembers.getCapacity() + " to " + poolSize + " Current Stack Size: " + freeMembers.size());
            }
        }
    }

    private synchronized void cleanupPool(boolean checkFreeMembers) {
        int i, len, poolLen = poolMembers.length;
        PoolMemberHolder<E> poolMember;
        if(checkFreeMembers) {
            List<PoolMemberHolder<E>> l = new ArrayList<AsyncObjectPool.PoolMemberHolder<E>>(poolLen);
            freeMembers.drainTo(l);
            len = l.size();
            for(i = 0; i < len; i++) {
                poolMember = l.get(i);
                if(poolMember.removalStatus == AVAILABLE) {
                    returnToPool(poolMember.member, false);
                }
                else {
                    poolMember.removalStatus = REMOVED;
                }
            }
        }
        PoolMemberHolder<E> newPoolMembers[] = Arrays.copyOf(poolMembers, poolLen);
        int newLen = 0;
        for(i = 0; i < poolLen; i++) {
            poolMember = poolMembers[i];
            if(poolMember.removalStatus == REMOVED) {
                try {
                    objectManager.destroyObject(poolMember.member);
                }
                catch(Exception e) {
                    log.error("Ignoring Exception while cleaning up pool member " + poolMember.member, e);
                }
            }
            else {
                newPoolMembers[newLen] = poolMember;
                newLen++;
            }
        }
        newPoolMembers = Arrays.copyOf(newPoolMembers, newLen);
        poolMembers = newPoolMembers;
        poolMemberList = new ArrayWrappingList<AsyncObjectPool.PoolMemberHolder<E>>(poolMembers);
        updateStackCapacity();
    }

    private void returnToPool(E member, boolean validate) {
        PoolMemberHolder<E>[] pms = poolMembers;
        int index = findMember(pms, member);
        if(index < 0) {
            throw new IllegalArgumentException("Returned item + " + member + " not pool member");
        }
        PoolMemberHolder<E> poolMemberHolder = pms[index];
        if(validate && !poolMemberHolder.isInUse) {
            throw new IllegalArgumentException("Member returned to pool is not Borrowed: " + member);
        }
        poolMemberHolder.changeInUseStatus(false);
        try {
            objectManager.beforeReturn(member);
        }
        catch(Exception e) {
            log.error("Error in onBorrow callback. Ignoring", e);
        }
        int removalStatus = poolMemberHolder.removalStatus;
        boolean checkFreeMembersForCleanup = false;
        if(removalStatus == AVAILABLE) {
            checkFreeMembersForCleanup = true;
            PoolUser<E> poolUser;
            while((poolUser = waitingUsers.poll()) != null) {
                if(poolUser.assignObject(poolMemberHolder, taskExecutor)) {
                    break;
                }
            }
            if(poolUser == null) {
                freeMembers.offerFirst(poolMemberHolder);
                removalStatus = poolMemberHolder.removalStatus;
                if(removalStatus == AVAILABLE && waitingUsers.size() > 0) {
                    ensureEmptyWaitersOrMembers();
                }
            }
        }
        else {
            poolMemberHolder.removalStatus = REMOVED;
        }
        if(removalStatus != AVAILABLE) {
            cleanupPool(checkFreeMembersForCleanup);
        }
    }

    /**
     * Return a member Object back to the pool
     * @param member Object to return
     */
    public void returnToPool(E member) {
        returnToPool(member, true);
    }

    /**
     * Change the pool size
     * @param newSize New Pool Size
     */
    public synchronized void setPoolSize(int newSize) {
        int i, diff;
        if(newSize < 1 || newSize > 1000) {
            throw new IllegalArgumentException("Illegal value of poolSize: " + newSize + " should be 1-1000");
        }
        PoolMemberHolder<E> holder;
        if(newSize > poolSize) {
            diff = newSize - poolSize;
            PoolMemberHolder<E> additionalPoolMembers[] = Arrays.copyOf(poolMembers, diff);
            PoolMemberHolder<E> newPoolMembers[] = Arrays.copyOf(poolMembers, poolMembers.length + diff);
            for(i = 0; i < diff; i++) {
                holder = new PoolMemberHolder<E>();
                holder.member = objectManager.createObject();
                additionalPoolMembers[i] = holder;
            }
            System.arraycopy(additionalPoolMembers, 0, newPoolMembers, poolMembers.length, diff);
            Arrays.sort(newPoolMembers);
            poolMembers = newPoolMembers;
            poolMemberList = new ArrayWrappingList<AsyncObjectPool.PoolMemberHolder<E>>(poolMembers);
            poolSize = newSize;
            for(i = 0; i < diff; i++) {
                returnToPool(additionalPoolMembers[i].member, false);
            }
            updateStackCapacity();
        }
        else if(newSize < poolSize) {
            diff = poolSize - newSize;
            List<PoolMemberHolder<E>> l = new ArrayList<AsyncObjectPool.PoolMemberHolder<E>>(diff);
            freeMembers.drainTo(l, diff);
            int len = l.size();
            int remaining = diff - len;
            for(i = 0; i < len; i++) {
                holder = l.get(i);
                holder.isInUse = false; // This line Ideally not required
                if(holder.removalStatus != AVAILABLE) {
                    remaining++;
                }
                holder.removalStatus = REMOVED;
            }
            boolean checkFreeMembersForCleanup = false;
            for(i = 0; remaining > 0 && i < poolMembers.length; i++) {
                holder = poolMembers[i];
                if(holder.removalStatus == AVAILABLE) {
                    checkFreeMembersForCleanup = true;
                    holder.removalStatus = MARKED;
                    remaining--;
                }
            }
            poolSize = newSize;
            cleanupPool(checkFreeMembersForCleanup);
        }
    }

    /**
     * Iterator over the current pool members.
     * @return Iterator of PoolMembers
     */
    protected Iterator<PoolMemberHolder<E>> memberIterator() {
        return poolMemberList.iterator();
    }

    /**
     * Get the current pool size.
     * @return Pool Size
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Prints a message using System.out.println giving the number of waiting users and free member Objects.
     */
    public void printStatus() {
        System.out.println("Waiting users = " + waitingUsers.size() + " free members = " + freeMembers.size());
    }
}
