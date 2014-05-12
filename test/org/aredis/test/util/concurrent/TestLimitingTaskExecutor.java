package org.aredis.test.util.concurrent;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.aredis.util.concurrent.LimitingTaskExecutor;

public class TestLimitingTaskExecutor {

    public static class Task implements Runnable {
        private int sleepTime = 5000;

        private String id;

        public String npeStr = "hello";

        public Task(String pid, int psleepTime) {
            id = pid;
            sleepTime = psleepTime;
        }

        public Task(String pid) {
            id = pid;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Completed Task: " + id);
            if (npeStr.length() > 100) {
                return;
            }
        }
    }

    public static class Task1 implements Runnable {

        public boolean taskRun;

        @Override
        public void run() {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
            taskRun = true;
        }

    }

    private static LinkedBlockingQueue<Task1> task1List = new LinkedBlockingQueue<Task1>();

    public static class TaskSubmitter extends Thread {

        private volatile boolean stop;

        private LimitingTaskExecutor executor;

        public TaskSubmitter(LimitingTaskExecutor pexecutor) {
            executor = pexecutor;
        }

        @Override
        public void run() {
            int i;
            do {
                for (i = 0; i < 50; i++) {
                    Task1 task1 = new Task1();
                    task1List.offer(task1);
                    executor.execute(task1);
                }
                synchronized (this) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                    }
                }
            } while (!stop);
        }

        public synchronized void wakeUp(boolean done) {
            if (done) {
                stop = true;
            }
            notifyAll();
        }
    }

    // Basic Test
    public static void main(String [] args) throws Exception {
        ThreadPoolExecutor t = new ThreadPoolExecutor(15, 15, 500, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        LimitingTaskExecutor t1 = new LimitingTaskExecutor(t, 10);
        LimitingTaskExecutor t2 = new LimitingTaskExecutor(t, 10);
        int i;
        for (i = 0; i < 15; i++) {
            t1.submit(new Task("one_" + i));
            t2.submit(new Task("two_" + i));
        }
        Thread.sleep(4800);
        t1.setTaskLimit(3);
        Thread.sleep(4800);
        t2.setTaskLimit(5);
        t.shutdown();
    }

    // Below shows Task Rejection By LimitingTaskExecutor when there are no tasks submitted yet and the
    // underlying ExecutorService rejects the task
    public static void main2(String [] args) throws Exception {
        ThreadPoolExecutor t = new ThreadPoolExecutor(10, 10, 500, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(2));
        // ThreadPoolExecutor t = new ThreadPoolExecutor(10, 10, 500, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(2), new ThreadPoolExecutor.DiscardPolicy());
        /* Below is an example of how to use discard policy on ThreadPoolExecutor when using with LimitingTaskExecutor
        ThreadPoolExecutor t = new ThreadPoolExecutor(10, 10, 500, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(2), new RejectedExecutionHandler() {

            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                if (r instanceof TaskWrapper) {
                    ((TaskWrapper) r).rejectExecution();
                }
            }
        });
        */
        int i;
        for (i = 0; i < 12; i++) {
            t.submit(new Task("main_" + i));
        }
        LimitingTaskExecutor t1 = new LimitingTaskExecutor(t, 5);
        for (i = 0; i < 3; i++) {
            try {
                t1.submit(new Task("one_" + i));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        t.shutdown();
    }

    // Below example tests the case when the underlying ExecutorService starts rejects tasks but the
    // LimitingTaskExecutor queues the rejected tasks and processes them using the one task submitted to it
    public static void main3(String [] args) throws Exception {
        ThreadPoolExecutor t = new ThreadPoolExecutor(10, 10, 500, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(2));
        LimitingTaskExecutor t1 = new LimitingTaskExecutor(t, 5);
        Task task = new Task("one_s");
        task.npeStr = null;
        t1.execute(task);
        int i;
        for (i = 0; i < 11; i++) {
            t.submit(new Task("main_" + i));
        }
        for (i = 0; i < 10; i++) {
            try {
                t1.submit(new Task("one_" + i));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        // Commenting out the below will cause all one_ tasks to complete one by one since the underlying
        // pool is shutdown preventing additional tasks to be created
        Thread.sleep(15000);
        t.shutdown();
    }

    // Concurrency test of 100 parallel Task submissions
    public static void main4(String [] args) throws Exception {
        int i, j, delay = 0;
        ThreadPoolExecutor t = new ThreadPoolExecutor(12, 12, 500, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(3));
        LimitingTaskExecutor t1 = new LimitingTaskExecutor(t, 10, new LinkedBlockingQueue<Runnable>());
        TaskSubmitter [] submitters = new TaskSubmitter[100];
        for (i = 0; i < submitters.length; i++) {
            submitters[i] = new TaskSubmitter(t1);
        }
        for (i = 0; i < 14; i++) {
            if ((i & 1) == 0) {
                delay += 1000;
            }
            t.execute(new Task("m" + i, delay));
        }
        for (i = 0; i < submitters.length; i++) {
            submitters[i].start();
        }
        for (i = 0; i < 5; i++) {
            Thread.sleep(1000);
            for (j = 0; j < submitters.length; j++) {
                submitters[j].wakeUp(false);
            }
        }
        Thread.sleep(100);
        for (i = 0; i < submitters.length; i++) {
            submitters[i].wakeUp(true);
        }
        t1.shutdown();
        // t.shutdown();
        System.out.println("Termination Status1 : " + t1.awaitTermination(200, TimeUnit.MILLISECONDS));
        System.out.println("Termination Status2 : " + t1.awaitTermination(300, TimeUnit.SECONDS));
        boolean verified = true;
        for (Task1 task1 : task1List) {
            i++;
            if (!task1.taskRun) {
                verified = false;
                System.out.println("Task not run for i = " + i + " Exitting.");
                break;
            }
        }
        if (verified) {
            System.out.println("Verified " + i + " tasks");
        }
    }

}
