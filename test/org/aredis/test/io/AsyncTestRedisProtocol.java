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

package org.aredis.test.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.TimeUnit;

import org.aredis.cache.AsyncHandler;
import org.aredis.io.ReusableByteArrayOutputStream;
import org.aredis.net.AsyncJavaSocketTransport;
import org.aredis.net.AsyncSocketTransport;
import org.aredis.net.AsyncSocketTransportFactory;

public class AsyncTestRedisProtocol {

    private static class ResultThread extends Thread {

        private AsynchronousSocketChannel channel;

        public ResultThread(AsynchronousSocketChannel pchannel) {
            channel = pchannel;
        }

        public void run() {
            try {
                ByteBuffer readBuffer = ByteBuffer.allocate(1024);
                do {
                    readBuffer.clear();
                    channel.read(readBuffer).get();
                    System.out.println("Message: " + new String(readBuffer.array(), 0, readBuffer.position()));
                } while(true);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class ReadHandler implements CompletionHandler<Integer, ByteBuffer> {

        private AsynchronousSocketChannel channel;

        public ReadHandler(AsynchronousSocketChannel pchannel) {
            channel = pchannel;
        }

        @Override
        public void completed(Integer result, ByteBuffer readBuffer) {
            System.out.println("Message: " + new String(readBuffer.array(), 0, readBuffer.position()));
            readBuffer.clear();
            channel.read(readBuffer, 15, TimeUnit.MINUTES, readBuffer, this);
        }

        @Override
        public void failed(Throwable exc, ByteBuffer attachment) {
            System.out.println("Got FAILURE");
            if(exc != null) {
                exc.printStackTrace();
            }
        }

    }

    private static class WriteHandler implements CompletionHandler<Integer, ByteBuffer> {

        private Boolean status = null;

        private AsynchronousSocketChannel channel;

        public WriteHandler(AsynchronousSocketChannel pchannel) {
            channel = pchannel;
        }

        @Override
        public void completed(Integer result,
                ByteBuffer byteBuffer) {
            if(byteBuffer.remaining() == 0) {
                updateResult(true);
            }
            else {
                channel.write(byteBuffer, 45, TimeUnit.MINUTES, byteBuffer, this);
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            if(exc != null) {
                exc.printStackTrace();
            }
            updateResult(false);
        }

        private synchronized void updateResult(boolean result) {
            status = result;
            notifyAll();
        }

        public synchronized Boolean awaitResult() {
            try {
                while(status == null) {
                    wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return status;
        }
    }

    private static class ConnectHandler implements AsyncHandler<Boolean> {

        private volatile boolean isComplete;

        private Throwable e;

        @Override
        public synchronized void completed(Boolean result, Throwable pe) {
            isComplete = true;
            e = pe;
            notifyAll();
        }

    }

    private static class ReadHandler1 implements AsyncHandler<Integer> {

        private AsyncSocketTransport con;

        private ReusableByteArrayOutputStream bop;

        public ReadHandler1(AsyncSocketTransport pcon) {
            con = pcon;
            bop = new ReusableByteArrayOutputStream();
        }

        @Override
        public void completed(Integer result, Throwable e) {
            if(result <= 0) {
                System.out.println("Got invalid result " + result);
            }
            else if(e != null) {
                System.out.println("Error in Write");
                e.printStackTrace();
            }
            else {
                System.out.print(new String(bop.getBuf(), 0, bop.getCount()));
                bop.reset();
                while(con.read(bop, 1024, this) > 0) {
                    System.out.print(new String(bop.getBuf(), 0, bop.getCount()));
                    bop.reset();
                }
            }
        }

    }

    private static class WriteHandler1 implements AsyncHandler<Integer> {

        private AsyncSocketTransport con;

        private FlushHandler flushHandler;

        private boolean writeComplete;

        private boolean isWriteError;

        public WriteHandler1(AsyncSocketTransport pcon) {
            con = pcon;
            flushHandler = new FlushHandler(pcon, this);
        }

        @Override
        public void completed(Integer result, Throwable e) {
            boolean isWriteComplete = true;
            boolean isError = true;
            if(result <= 0) { // Not required, just for testing AsyncSocketTransport
                System.out.println("Got invalid result " + result);
            }
            else if(e != null) {
                System.out.println("Error in Write");
                e.printStackTrace();
            }
            else {
                isError = false;
                if(con.requiresFlush()) {
                    isWriteComplete = false;
                    con.flush(flushHandler);
                }
            }
            if(isWriteComplete) {
                notifyWrite(isError);
            }
        }

        public void resetWait() {
            writeComplete = false;
            isWriteError = false;
        }

        public synchronized void awaitWrite() throws IOException {
            try {
                while(!writeComplete) {
                    wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(isWriteError) {
                throw new IOException("ERROR DURING ASYNC WRITE");
            }
        }

        public synchronized void notifyWrite(boolean isError) {
            writeComplete = true;
            isWriteError = isError;
            notifyAll();
        }

        public void flush() {
            if(con.requiresFlush()) {
                con.flush(flushHandler);
            }
            else {
                notifyWrite(false);
            }
        }
    }

    private static class FlushHandler implements AsyncHandler<Integer> {

        private AsyncSocketTransport con;

        private WriteHandler1 writeHandler1;

        public FlushHandler(AsyncSocketTransport pcon, WriteHandler1 pwriteHandler1) {
            con = pcon;
            writeHandler1 = pwriteHandler1;
        }

        @Override
        public void completed(Integer result, Throwable e) {
            if(result > 0) {
                con.flush(this);
            }
            else {
                boolean isError = true;
                if(result == 0) {
                    System.out.println("Got invalid result " + result);
                }
                else if(e != null) {
                    System.out.println("Error in Write");
                    e.printStackTrace();
                }
                else {
                    isError = false;
                }
                writeHandler1.notifyWrite(isError);
            }
        }

    }

    public static void main1(String args[]) throws Exception {
        ReusableByteArrayOutputStream bop = new ReusableByteArrayOutputStream();
        PrintStream out = null;
        String host = "localhost";
        int port = 6379;
        if(args.length > 0) {
            host = args[0];
        }
        if(args.length > 1) {
            port = Integer.parseInt(args[1]);
        }
        InetSocketAddress address = new InetSocketAddress(host, port);
        AsynchronousChannelGroup channelGroup = AsyncJavaSocketTransport.getChannelGroup();
        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(channelGroup);
        channel.connect(address).get();
        // ResultThread resultThread = new ResultThread(channel);
        // resultThread.start();
        ReadHandler readHandler = new ReadHandler(channel);
        ByteBuffer readBuffer = ByteBuffer.allocate(1024);
        channel.read(readBuffer, 15, TimeUnit.MINUTES, readBuffer, readHandler);
        out = new PrintStream(bop);

        int i;
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        String fromUser;
        while ((fromUser = stdIn.readLine()) != null) {
            bop.reset();
            String [] pars = fromUser.split(" ");
            out.println("*" + pars.length);
            for(i = 0; i < pars.length; i++) {
                if("$null".equals(pars[i])) {
                    out.println("$0");
                    out.println();
                }
                else {
                    out.println("$" + pars[i].length());
                    out.println(pars[i]);
                }
            }
            out.flush();
            System.out.println("WRITING NEXT COMMAND");
            WriteHandler writeHandler = new WriteHandler(channel);
            ByteBuffer message = ByteBuffer.wrap(bop.getBuf(), 0, bop.getCount());
            channel.write(message, 30, TimeUnit.MINUTES, message, writeHandler);
            boolean written = writeHandler.awaitResult();
            if(!written) {
                System.out.println("Write failed");
                break;
            }
        }
    }

    public static void main(String args[]) throws Exception {
        AsyncSocketTransport con = null;
        ReusableByteArrayOutputStream bop = new ReusableByteArrayOutputStream();
        PrintStream out = null;
        String host = "localhost";
        int port = 6379;
        if(args.length > 0) {
            host = args[0];
        }
        if(args.length > 1) {
            port = Integer.parseInt(args[1]);
        }
        ConnectHandler connectHandler = new ConnectHandler();
        AsyncSocketTransportFactory asyncSocketTransportFactory = AsyncSocketTransportFactory.getDefault();
        con = asyncSocketTransportFactory.getTransport(host, port);
        con.connect(connectHandler);
        synchronized(connectHandler) {
            while(!connectHandler.isComplete) {
                connectHandler.wait();
            }
        }
        if(connectHandler.e != null) {
            throw new IOException("Error Connecting", connectHandler.e);
        }
        // ResultThread resultThread = new ResultThread(channel);
        // resultThread.start();
        ReadHandler1 readHandler1 = new ReadHandler1(con);
        con.read(readHandler1.bop, 1024, readHandler1);
        WriteHandler1 writeHandler1 = new WriteHandler1(con);
        out = new PrintStream(bop);

        int i;
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        String fromUser;
        while ((fromUser = stdIn.readLine()) != null) {
            bop.reset();
            String [] pars = fromUser.split(" ");
            out.println("*" + pars.length);
            for(i = 0; i < pars.length; i++) {
                if("$null".equals(pars[i])) {
                    out.println("$0");
                    out.println();
                }
                else {
                    out.println("$" + pars[i].length());
                    out.println(pars[i]);
                }
            }
            out.flush();
            System.out.println("WRITING NEXT COMMAND");
            writeHandler1.resetWait();
            if(con.write(bop.getBuf(), 0, bop.getCount(), writeHandler1) > 0) {
                writeHandler1.flush();
            }
            writeHandler1.awaitWrite();
        }
    }
}
