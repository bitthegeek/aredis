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

package org.aredis.test.cache;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

public class RedisPipelineTry {

    private static boolean stop;

    private static String [] args;

    private PrintWriter out;

    private BufferedReader in;

    private volatile int writeCount;

    private volatile int readCount;

    private int limitPending;

    private int maxPending;

    public RedisPipelineTry(PrintWriter pout, BufferedReader pin) {
        out = pout;
        in = pin;
        limitPending = Integer.MAX_VALUE;
    }

    private volatile boolean writerWaiting;

    private void waitForClearup() {
        if(writeCount - readCount >= limitPending) {
            out.flush();
            writerWaiting = true;
            synchronized(this) {
                while(writeCount - readCount >= limitPending) {
                    try {
                        wait();
                    }
                    catch(InterruptedException e){};
                }
                writerWaiting = false;
            }
        }
    }

    private void notifyClearup() {
        if(writeCount - readCount < limitPending && writerWaiting) {
            synchronized(this) {
                notifyAll();
            }
        }
    }

    private void writeCommand(String command) {
        int i;
        String [] pars = command.split(" ");
        try {
        out.print("*" + pars.length); out.print("\r\n");
        for(i = 0; i < pars.length; i++) {
            if("$null".equals(pars[i])) {
                out.print("$0"); out.print("\r\n");
                out.print("\r\n");
            }
            else {
                out.print("$" + pars[i].length()); out.print("\r\n");
                out.print(pars[i]); out.print("\r\n");
            }
        }
        writeCount++;
        int newMaxPending = writeCount - readCount;
        if(newMaxPending > maxPending) {
            maxPending = newMaxPending;
        }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private Object readResult() {
        Object result = null;
        String fromServer;
        int i, bytesRead, count;
        try {
        fromServer = in.readLine();
        char ch = fromServer.charAt(0);
        if(ch != '*' && ch != '$') {
            result = fromServer;
        }
        else {
            int numResults = 1;
            if(ch == '*') {
                numResults = Integer.parseInt(fromServer.substring(1));
            }
            List<String> results = new ArrayList<String>(numResults);
            for(i = 0; i < numResults; i++) {
                if(ch == '*') {
                    fromServer = in.readLine();
                }
                count = Integer.parseInt(fromServer.substring(1));
                char [] b = new char[count];
                bytesRead = 0;
                do {
                    bytesRead += in.read(b, bytesRead, count - bytesRead);
                } while(bytesRead < count);
                fromServer = new String(b);
                results.add(fromServer);
                in.readLine();
            }
            if(numResults == 1) {
                result = results.get(0);
            }
            else {
                result = results;
            }
        }
        readCount++;
        }
        catch(Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    private static class WriteThread extends Thread {
        private RedisPipelineTry p;

        public WriteThread(RedisPipelineTry pp) {
            p = pp;
        }

        public void run() {
            boolean done = false;
            do {
                done = p.stop;
                // Note there is still a corner case where the
                // below final command may not be read. But this is only a test
                p.writeCommand("set hello world" + p.writeCount);
                p.writeCommand("get hello");
                if(!done) {
                    p.waitForClearup();
                }
            } while(!done);
            p.out.flush();
        }
    }

    private static class ReadThread extends Thread {
        private RedisPipelineTry p;

        public ReadThread(RedisPipelineTry pp) {
            p = pp;
        }

        public void run() {
            while(p.readCount < p.writeCount || !p.stop) {
                String result = (String) p.readResult();
                if(p.readCount < 20) {
                    System.out.println("Result0 = " + result);
                }
                result = (String) p.readResult();
                if(p.readCount < 20) {
                    System.out.println("Result = " + result);
                }
                p.notifyClearup();
            }
        }
    }

    private static class CommandHolder implements Callable<String> {
        private String command;
        private String result;
        FutureTask<String> futureResult;

        @Override
        public String call() throws Exception {
            return result;
        }
    }

    private static class WriteFromQThread extends Thread {
        private RedisPipelineTry p;
        private BlockingQueue<CommandHolder> q;
        private BlockingQueue<CommandHolder> readQ;

        public WriteFromQThread(RedisPipelineTry pp) {
            q = new LinkedBlockingQueue<CommandHolder>();
            p = pp;
        }

        public void run() {
            boolean done = false;
            do {
                try {
                    CommandHolder ch = q.take();
                    String command = ch.command;
                    // System.out.println("Sending " + command);
                    p.writeCommand(command);
                    readQ.add(ch);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                done = p.stop;
                if(!done) {
                    p.waitForClearup();
                }
            } while(!done);
            p.out.flush();
        }
    }

    private static class ReadFromQThread extends Thread {
        private RedisPipelineTry p;
        private BlockingQueue<CommandHolder> q;

        public ReadFromQThread(RedisPipelineTry pp) {
            q = new LinkedBlockingQueue<CommandHolder>();
            p = pp;
        }

        public void run() {
            while(p.readCount < p.writeCount || !p.stop) {
                String result = (String) p.readResult();
                if(p.readCount < 20) {
                    System.out.println("Result0 = " + result);
                }
                try {
                    CommandHolder ch = q.take();
                    ch.result = result;
                    ch.futureResult.run();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                p.notifyClearup();
            }
        }
    }

    private static class FutureTest extends Thread {
        private String suffix;
        private BlockingQueue<CommandHolder> q;

        public void run() {
            boolean stopped = false;
            int i = 0;
            StringBuilder sb = new StringBuilder();
            String key = "hello" + suffix;
            String setexCommandPrefix = "setex " + key + " 30 ";
            String getCommand = "get " + key;
            do {
                stopped = RedisPipelineTry.stop;
                String val = String.valueOf(i);
                CommandHolder ch = new CommandHolder();
                sb.setLength(0);
                ch.command = sb.append(setexCommandPrefix).append(val).toString();
                FutureTask<String> futureResult = new FutureTask<String>(ch);
                ch.futureResult = futureResult;
                q.add(ch);
                ch = new CommandHolder();
                ch.command = getCommand;
                futureResult = new FutureTask<String>(ch);
                ch.futureResult = futureResult;
                q.add(ch);
                try {
                    String val1 = futureResult.get();
                    if(!val.equals(val1)) {
                        System.out.println("Comapare v = " + val + " and " + val1 + " failed");
                        break;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                i++;
            } while(!stopped);
        }
    }

    public static void main(String[] args) throws Exception {

        int i;
        Socket kkSocket = null;
        PrintWriter out = null;
        BufferedReader in = null;
        InputStream sockIn = null;

        long start = System.currentTimeMillis();
        try {
            String host = "localhost";
            int port = 6379;
            if(args.length > 0) {
                host = args[0];
            }
            if(args.length > 1) {
                port = Integer.parseInt(args[1]);
            }
            kkSocket = new Socket(host, port);
            out = new PrintWriter(new BufferedOutputStream(kkSocket.getOutputStream()));
            sockIn = kkSocket.getInputStream();
            in = new BufferedReader(new InputStreamReader(new BufferedInputStream(sockIn)));
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host: taranis.");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to: taranis.");
            System.exit(1);
        }

        RedisPipelineTry redisTry = new RedisPipelineTry(out, in);
        if(args.length > 2) {
            redisTry.limitPending = Integer.parseInt(args[2]);
        }
        redisTry.limitPending = 5;
        ReadFromQThread readThread = new ReadFromQThread(redisTry);
        WriteFromQThread writeThread = new WriteFromQThread(redisTry);
        writeThread.readQ = readThread.q;
        readThread.start();
        writeThread.start();

        int numThreads = 10;
        FutureTest [] futureTests = new FutureTest[numThreads];

        for(i = 0; i < numThreads; i++) {
            futureTests[i] = new FutureTest();
            futureTests[i].q = writeThread.q;
            futureTests[i].suffix = String.valueOf(i);
            futureTests[i].start();
        }

        Thread.sleep(20000);
        stop = true;
        writeThread.join();
        System.out.println("Write Thread Completed");
        readThread.join();
        System.out.println("Read Thread Completed");

        for(i = 0; i < numThreads; i++) {
            futureTests[i].join();
        }

        out.close();
        in.close();
        kkSocket.close();
        long diff = System.currentTimeMillis() - start;
        System.out.println("COUNT = " + redisTry.readCount + " in " + diff + " ms = " + (redisTry.readCount * 1000L / diff) + " rps " + " max pending " + redisTry.maxPending);
    }

    public static void mainSimple(String[] args) throws Exception {

        int i;
        Socket kkSocket = null;
        PrintWriter out = null;
        BufferedReader in = null;
        InputStream sockIn = null;

        long start = System.currentTimeMillis();
        try {
            String host = "localhost";
            int port = 6379;
            if(args.length > 0) {
                host = args[0];
            }
            if(args.length > 1) {
                port = Integer.parseInt(args[1]);
            }
            kkSocket = new Socket(host, port);
            out = new PrintWriter(new BufferedOutputStream(kkSocket.getOutputStream()));
            sockIn = kkSocket.getInputStream();
            in = new BufferedReader(new InputStreamReader(new BufferedInputStream(sockIn)));
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host: taranis.");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to: taranis.");
            System.exit(1);
        }

        RedisPipelineTry redisTry = new RedisPipelineTry(out, in);
        if(args.length > 2) {
            redisTry.limitPending = Integer.parseInt(args[2]);
        }
        ReadThread readThread = new ReadThread(redisTry);
        WriteThread writeThread = new WriteThread(redisTry);
        readThread.start();
        writeThread.start();

        Thread.sleep(20000);
        stop = true;
        writeThread.join();
        readThread.join();

        out.close();
        in.close();
        kkSocket.close();
        long diff = System.currentTimeMillis() - start;
        System.out.println("COUNT = " + redisTry.readCount + " in " + diff + " ms = " + (redisTry.readCount * 1000L / diff) + " rps " + " max pending " + redisTry.maxPending);
    }

}
