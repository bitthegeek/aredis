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
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>
 * This class is a utility class implementing ExecutorService which can sit on top of any ExecutorService
 * like ThreadPoolExecutor and limit the number of concurrent tasks submitted to it using a configured limit.
 * </p>
 *
 * <p>
 * It sort of provides a fixed size view of a global ThreadPool of higher capacity instead of creating a
 * new Fixed Size Thread Pool. It prevents eating into threads of the underlying Thread Pool when the
 * concurrent tasks exceed the specified limit by queuing the tasks.
 * </p>
 *
 * <p>
 * It will be useful for purposes like providing a small size ThreadPool for message subscription from a
 * global Thread Pool.
 * </p>
 *
 * <p>
 * Notes:
 * </p>
 *
 * <p>
 * Each task executed is run by an instance of {@link TaskWrapper}. The TaskWrapper which
 * is submitted to the underlying executorService re-uses the same run method for running
 * any tasks in LimitingTaskExecutors queue. So the competedTasks count of the underlying
 * executorService may not indicate the correct number of tasks processed from the LimitingTaskExecutor.
 * </p>
 *
 * <p>
 * When the underlying executorService rejects a task it is queued if there is at least one
 * outstanding task submitted successfully to de-queue it. Otherwise the task is rejected
 * with the LimitingExecutor's configured RejectedTaskHandler. Rejections by the underlying
 * executorSerice are detected by handling the {@link RejectedExecutionException} in case
 * of the default configuration or by using a ThreadLocal in case of {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy}.
 * Note that you should not use the {@link java.util.concurrent.ThreadPoolExecutor.DiscardPolicy}
 * handler because LimitingTaskExecutor cannot detect task rejection in that case. If you want to configure
 * a discard policy for the underlying executorService configure your own {@link RejectedExecutionHandler}
 * and include a call to {@link TaskWrapper#rejectExecution} in it as below:
 * </p>
 *
 * <pre>
 * <code>
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                if (r instanceof TaskWrapper) {
                    ((TaskWrapper) r).rejectExecution();
                }
            }
 * </code>
 * </pre>
 *
 * <p>
 * TaskRejection can also happen when submitting a task when the max allowed limit of tasks
 * are already submitted and the queue is full. The default queue is configured with a limit
 * of 150. A task is also rejected when the LimitingTaskExecutor or the underlying executorService
 * is shutdown. In all cases the Configured {@link RejectedTaskHandler}'s rejectedExecution method is called.
 * The default handler is ABORT_POLICY which throws a {@link RejectedExecutionException}.
 * </p>
 *
 * <p>
 * The number of outstanding tasks submitted to the underlying executorService is usually within the specified
 * limit though in some corner cases it could exceed the limit.
 * </p>
 *
 * @author Suresh
 *
 */
public class LimitingTaskExecutor extends AbstractExecutorService {

    private static ThreadLocal<Boolean> inPlaceDetecter = new ThreadLocal<Boolean>();

    /**
     * TaskWrapper responsible for running the tasks which is submitted to the underlying
     * ExecutorService. This class is declared as public so that the rejectExecution method
     * can be called by the underlying executorService when rejecting an instance of TaskWrapper
     * to indicate rejection of the task.
     *
     * @author Suresh
     *
     */
    public class TaskWrapper implements Runnable {
        private Runnable task;

        private boolean isTaskRejected;

        private boolean isExternalThread;

        private TaskWrapper(Runnable ptask) {
            task = ptask;
        }

        /**
         * Marks the submitted task as rejected so that the LimitingTask executor can take
         * an appropriate action to queue the task or reject it if the queue is full.
         */
        public void rejectExecution() {
            isTaskRejected = true;
        }

        @Override
        public void run() {
            Runnable nextTask = task;
            do {
                boolean taskSubmitted = false;
                boolean taskSuccessful = false;
                try {
                    if (inPlaceDetecter.get() == null) {
                        taskSubmitted = true;
                        nextTask.run();
                        taskSuccessful = true;
                    } else {
                        isTaskRejected = true;
                    }
                } finally {
                    if (taskSubmitted) {
                        completedTaskCounter.incrementAndGet();
                        nextTask = null;
                        try {
                            nextTask = checkForQueuedTasksAtEnd(nextTask != EmptyRunnable.instance);
                            if (nextTask != null) {
                                if (!taskSuccessful || isExternalThread) {
                                    // Since there is an Exception in current Thread propagate it and do not
                                    // re-use this thread. Submit a new task or if that is also Rejected
                                    // which is the worst corner case, start a new Thread.
                                    if (executeTask(nextTask)) {
                                        nextTask = null;
                                    } else {
                                        if (!taskSuccessful) {
                                            nextTask = null;
                                            TaskWrapper tw = new TaskWrapper(nextTask);
                                            tw.isExternalThread = true;
                                            new Thread(tw).start();
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                        }
                    }
                }
                if (nextTask != null) {
                    task = nextTask;
                }
            } while (nextTask != null);
        }
    }

    /**
     * RejectedTaskHandler which Aborts the rejected task by throwing a RejectedExecutionException.
     * This is the default handler.
     */
    public static final RejectedTaskHandler ABORT_POLICY = new RejectedTaskHandler() {

        @Override
        public void rejectedExecution(Runnable r, LimitingTaskExecutor e) {
            throw new RejectedExecutionException("Task " + r.toString() +
                                                 " rejected from " +
                                                 e.toString());
        }

    };

    /**
     * RejectedTaskHandler which runs the rejected task in place by calling the run method
     * of the submitted task so that the task runs in the same thread as the caller effectively
     * slowing down the caller thread.
     */
    public static final RejectedTaskHandler RUN_IN_PLACE_POLICY = new RejectedTaskHandler() {

        @Override
        public void rejectedExecution(Runnable r, LimitingTaskExecutor e) {
            if (!e.isShutdown()) {
                r.run();
            }
        }

    };

    /**
     * RejectedTaskHandler which discards the rejected task by doing nothing.
     */
    public static final RejectedTaskHandler DISCARD_POLICY = new RejectedTaskHandler() {

        @Override
        public void rejectedExecution(Runnable r, LimitingTaskExecutor executor) {
        }

    };

    private ExecutorService executorService;

    private volatile int taskLimit;

    // Below is the taskCounter used to coordinate and keep track of the limit
    private AtomicInteger taskCounter;

    // Since a task may get rejected after a taskCounter has been incremented it is not a reliable indicator
    // of tasks which are submitted and are going to run. The below counter is for that purpose. The TaskWrapper
    // ensures that at a given time if the activeTaskCounter.get() > 0 any tasks in waitingTasks Q will be processed
    // (Unless shutDownNow is called)
    private AtomicInteger activeTaskCounter;

    private AtomicInteger completedTaskCounter;

    private BlockingQueue<Runnable> waitingTasks;

    private RejectedTaskHandler rejectedTaskHandler;

    private final ReentrantLock terminationLock = new ReentrantLock();

    /*
     * Wait condition to support awaitTermination
     */
    private final Condition terminationCondition = terminationLock.newCondition();

    private volatile boolean shutDownFlag;

    private volatile boolean terminatedFlag;

    /**
     * Creates a Limiting TaskExecutor.
     *
     * @param pexecutorService Underlying Executor Service used to submit the tasks to
     * @param ptaskLimit Task Limit. When this many tasks are running (i.e submitted but not completed) on
     *        the underlying Executor Service, new tasks are added to the waiting tasks Q
     * @param pwaitingTasks The Q to use for adding waiting tasks
     * @param prejectedTaskHandler Specifies what to do with tasks submitted when the max number of tasks
     *        are running and the waiting tasks Q is also full.
     */
    public LimitingTaskExecutor(ExecutorService pexecutorService, int ptaskLimit, BlockingQueue<Runnable> pwaitingTasks, RejectedTaskHandler prejectedTaskHandler) {
        if (pexecutorService == null) {
            throw new NullPointerException("ExecutorService is Null");
        }
        if (ptaskLimit <= 0 || ptaskLimit > 1000) {
            throw new IllegalArgumentException("Invalid taskLimit " + ptaskLimit + ". Should be between 1 and 1000");
        }
        if (pwaitingTasks == null) {
            throw new NullPointerException("Q for Waiting Tasks is Null");
        }
        if (prejectedTaskHandler == null) {
            throw new NullPointerException("rejectedTaskHandler is Null");
        }
        executorService = pexecutorService;
        taskLimit = ptaskLimit;
        taskCounter = new AtomicInteger();
        // A separate counter is created since a task can be rejected after submitting
        activeTaskCounter = new AtomicInteger();
        completedTaskCounter = new AtomicInteger();
        waitingTasks = pwaitingTasks;
        rejectedTaskHandler = prejectedTaskHandler;
    }

    /**
     * Creates a Limiting TaskExecutor with ABORT_POLICY as the RejectedExecutionPolicy.
     *
     * @param pexecutorService Underlying Executor Service used to submit the tasks to
     * @param ptaskLimit Task Limit. When this many tasks are running (i.e submitted but not completed) on
     *        the underlying Executor Service, new tasks are added to the waiting tasks Q
     * @param pwaitingTasks The Q to use for adding waiting tasks
     */
    public LimitingTaskExecutor(ExecutorService pexecutorService, int ptaskLimit, BlockingQueue<Runnable> pwaitingTasks) {
        this(pexecutorService, ptaskLimit, pwaitingTasks, ABORT_POLICY);
    }

    /**
     * Creates a Limiting TaskExecutor with ABORT_POLICY as the RejectedExecutionPolicy. A
     * LinkedBlockingQueue of capacity 150 is used as the waiting tasks Q.
     *
     * @param pexecutorService Underlying Executor Service used to submit the tasks to
     * @param ptaskLimit Task Limit. When this many tasks are running (i.e submitted but not completed) on
     *        the underlying Executor Service, new tasks are added to the waiting tasks Q
     */
    public LimitingTaskExecutor(ExecutorService pexecutorService, int ptaskLimit) {
        this(pexecutorService, ptaskLimit, new LinkedBlockingQueue<Runnable>(150), ABORT_POLICY);
    }

    /*
     * Runs command on the passed executor after wrapping it with a TaskWrapper. It also checks for Rejected Tasks
     * or Tasks which are attempted to be run in place of the calling thread and returns false in such cases to
     * indicate that the command has not been run.
     */
    private boolean executeTask(Runnable command) {
        boolean result = false;
        if (!executorService.isShutdown()) {
            try {
                inPlaceDetecter.set(Boolean.TRUE);
                TaskWrapper tw = new TaskWrapper(command);
                executorService.execute(tw);
                result = !tw.isTaskRejected;
                if (result) {
                    activeTaskCounter.incrementAndGet();
                }
            } catch (RejectedExecutionException e) {
                result = false;
            } finally {
                inPlaceDetecter.remove();
            }
        }

        return result;
    }

    private int checkAndIncrementTaskCounter() {
        int limit = taskLimit;
        int result;
        for (;;) {
            int current = taskCounter.get();
            if (current < limit) {
                result = current + 1;
                if (taskCounter.compareAndSet(current, result)) {
                    break;
                }
            } else {
                result = limit - current;
                break;
            }
        }

        return result;
    }

    private boolean checkAndNotifyTermination(int taskCount) {
        boolean terminated = false;
        terminated = terminatedFlag;
        if (!terminated && taskCount == 0 && isShutdown()) {
            terminationLock.lock();
            try {
                terminated = terminatedFlag;
                if (!terminated) {
                    terminated = true;
                    terminatedFlag = terminated;
                    terminationCondition.signalAll();
                }
            } finally {
                terminationLock.unlock();
            }
        }
        return terminated;
    }

    private boolean decrementTaskCounterAndCheckTermination() {
        int taskCount = taskCounter.decrementAndGet();
        boolean terminated = false;
        if (taskCount == 0) {
            terminated = checkAndNotifyTermination(taskCount);
        }
        return terminated;
    }

    private synchronized void ensureEmptyWaitersOrFullTaskCounter() {
        boolean tryAgain = false;
        int limit;
        do {
            limit = taskLimit;
            tryAgain = false;
            if(checkAndIncrementTaskCounter() > 0) {
                if ( waitingTasks.size() > 0) {
                    if (executeTask(EmptyRunnable.instance)) {
                        tryAgain = true;
                    } else {
                        decrementTaskCounterAndCheckTermination();
                        break;
                    }
                } else {
                    decrementTaskCounterAndCheckTermination();
                }
            }
        } while(tryAgain || taskCounter.get() < limit && waitingTasks.size() > 0);
    }

    private Runnable checkForQueuedTasksAtEnd(boolean addNewTasks) {
        int limit = taskLimit;
        int counter;
        Runnable task = null;
        do {
            boolean checkQ = true;
            counter = taskCounter.get();
            if (counter > limit) {
                checkQ = false;
                // Tasks have exceeded the limit due to a change in the limit or due to some corner case. So
                // Relinquish this task. However we should also ensure that the activeTaskCounter which is the
                // reliable indicator for running tasks does not become 0. If it does we will proceed to checking
                // for queued tasks after reversing the counter changes since this is the only task known to
                // be running.
                if (taskCounter.compareAndSet(counter, counter - 1)) {
                    if (activeTaskCounter.decrementAndGet() > 0) {
                        break;
                    } else {
                        checkQ = true;
                        taskCounter.incrementAndGet();
                        activeTaskCounter.incrementAndGet();
                    }
                }
            }
            if (checkQ) {
                // Check for Queued tasks
                task = waitingTasks.poll();
                if (task == null) {
                    decrementTaskCounterAndCheckTermination();
                    activeTaskCounter.decrementAndGet();
                    // A second check is done after the counters are decremented. This is to ensure that at
                    // any point a non zero activeTaskCounter means the queued tasks will be processed
                    task = waitingTasks.poll();
                    if (task != null) {
                        // The below increments can result in the tasks temporarily exceeding the limit which
                        // is Okay to ensure that queued tasks are taken up
                        taskCounter.incrementAndGet();
                        activeTaskCounter.incrementAndGet();
                    }
                }
                if (task != null && addNewTasks && counter < limit) {
                    ensureEmptyWaitersOrFullTaskCounter();
                }
                break;
            }
        } while (true);

        return task;
    }

    /**
     * Shuts down this LimitingTaskExecutor. New tasks are rejected but already submitted
     * and queued tasks are run normally. The underlying executorService is not shut down.
     * @see java.util.concurrent.ExecutorService#shutdown()
     */
    @Override
    public void shutdown() {
        shutDownFlag = true;
    }

    /**
     * In addition to shut down the task queue is emptied and returned. The submitted tasks
     * however are processed normally and not interrupted as mentioned in the spec.
     * @see java.util.concurrent.ExecutorService#shutdownNow()
     */
    @Override
    public List<Runnable> shutdownNow() {
        shutDownFlag = true;
        List<Runnable> tasks = new ArrayList<Runnable>(waitingTasks.size());
        return tasks;
    }

    /**
     * Return true if shutdown has been called on this LimitingTaskExecutor or the underlying
     * executorService is shutdown.
     * @see java.util.concurrent.ExecutorService#isShutdown()
     */
    @Override
    public boolean isShutdown() {
        return shutDownFlag || executorService.isShutdown();
    }

    /**
     * Checks if this LimitingTaskExecutor is terminated after a shutdown call or a shutdown
     * of the underlying executorService.
     * @see java.util.concurrent.ExecutorService#isTerminated()
     */
    @Override
    public boolean isTerminated() {
        return checkAndNotifyTermination(taskCounter.get());
    }

    /**
     * Calls awaitTermination on the underlying Executor Service.
     * @see java.util.concurrent.ExecutorService#awaitTermination(long, java.util.concurrent.TimeUnit)
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        boolean result = false;
        if (isShutdown()) {
            result = checkAndNotifyTermination(taskCounter.get());;
            if (!result) {
                long nanos = unit.toNanos(timeout);
                terminationLock.lock();
                try {
                    result = terminatedFlag;
                    if (!result) {
                        terminationCondition.awaitNanos(nanos);
                        result = terminatedFlag;
                    }
                }
                finally {
                    terminationLock.unlock();
                }
            }
        }

        return result;
    }

    /**
     * Executes the command by passing it on to the underlying executorService or queues it
     * for later execution if the number of outstanding tasks submitted to the executorService
     * is already the specified limit. Read the description about this class for more details.
     * @see java.util.concurrent.Executor#execute(java.lang.Runnable)
     */
    @Override
    public void execute(Runnable command) {
        boolean reject = false;
        boolean queuingSuccesful = false;
        boolean serviceRejection = false;
        if (command == null) {
            throw new NullPointerException("command is Null");
        }
        if (isShutdown()) {
            reject = true;
        } else if(checkAndIncrementTaskCounter() > 0) {
            if (isShutdown()) {
                reject = true;
                decrementTaskCounterAndCheckTermination();
            } else if (!executeTask(command)) {
                serviceRejection = true;
                // Underlying executor has rejected try to enqueue and accept the task
                decrementTaskCounterAndCheckTermination();
                queuingSuccesful = waitingTasks.offer(command);
                reject = !queuingSuccesful;
            }
        } else {
            queuingSuccesful = waitingTasks.offer(command);
            reject = !queuingSuccesful;
            if (queuingSuccesful) {
                if (taskCounter.get() < taskLimit) {
                    ensureEmptyWaitersOrFullTaskCounter();
                }
            }
        }
        if (queuingSuccesful && activeTaskCounter.get() <= 0) {
            if (!serviceRejection) {
                // Defensive code for an imaginary situation all taskCounters have been incremented by parallel
                // threads but not a single executeTask has been called yet to get a positive activeTaskCounter.
                // Since Queuing is successful trying to ensure an active task by submitting a new Task which
                // might result in the tasks temporarily exceeding the limit
                if (executeTask(EmptyRunnable.instance)) {
                    taskCounter.incrementAndGet();
                } else {
                    serviceRejection = true;
                }
            }
            // Even if task is queued reject if there is no activeTask to dequeue it later
            if (serviceRejection) {
                reject = waitingTasks.remove(command);
            }
        }
        if (reject) {
            rejectedTaskHandler.rejectedExecution(command, this);
        }
    }

    /**
     * Returns a string identifying this executor, as well as its state,
     * including indications of run state and estimated worker and
     * task counts. It also includes the toString of the underlying ExecutorService.
     *
     * @return a string identifying this pool, as well as its state and the toString of the underlying ExecutorService
     */
    public String toString() {
        String rs = "Running";
        if (isShutdown()) {
            rs = "Shutdown";
        }
        if (isTerminated()) {
            rs = "Terminated";
        }
        return super.toString() +
            "[" + rs +
            ", Task Limit = " + taskLimit +
            ", active threads = " + getNumRunningTasks() +
            ", queued tasks = " + waitingTasks.size() +
            ", completed tasks = " + completedTaskCounter.get() +
            "]. Uses: " + executorService.toString();
    }

    /**
     * Returns the Task Limit which specifies the max no. of outstanding tasks submitted to
     * the underlying executorService.
     * @return Task Limit
     */
    public int getTaskLimit() {
        return taskLimit;
    }

    /**
     * Sets a new value of the task limit which should be between 1 and 1000.
     * @param ptaskLimit New value of task limit
     */
    public synchronized void setTaskLimit(int ptaskLimit) {
        if (ptaskLimit <= 0 || ptaskLimit > 1000) {
            throw new IllegalArgumentException("Invalid taskLimit " + ptaskLimit + ". Should be between 1 and 1000");
        }
        taskLimit = ptaskLimit;
        if (taskCounter.get() < taskLimit && waitingTasks.size() > 0) {
            ensureEmptyWaitersOrFullTaskCounter();
        }
    }

    /**
     * Returns the queue used to enqueue tasks for later processing.
     * @return Waiting Task Queue
     */
    public BlockingQueue<Runnable> getWaitingTaskQueue() {
        return waitingTasks;
    }

    /**
     * Returns the number of outstanding tasks submitted to the underlying executorService.
     * @return Number of running tasks
     */
    public int getNumRunningTasks() {
        return activeTaskCounter.get();
    }

    /**
     * Returns the number of tasks that have been completed so far by this LimitingTaskExecutor
     * which is a continously increasing value.
     * @return Total Number of completed tasks
     */
    public int getNumCompetedTasks() {
        return completedTaskCounter.get();
    }
}
