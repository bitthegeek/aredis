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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.aredis.cache.AsyncHandler;
import org.aredis.cache.RedisRawResponse;
import org.aredis.cache.RedisResponseReader;
import org.aredis.io.ReusableByteArrayOutputStream;
import org.aredis.net.AsyncSocketTransport;
import org.aredis.net.AsyncSocketTransportFactory;

public class TestRedisResponseReader {

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

    private static class ResponseHandler implements AsyncHandler<RedisRawResponse> {

        private RedisResponseReader responseReader;

        public ResponseHandler(RedisResponseReader presponseReader) {
            responseReader = presponseReader;
        }

        @Override
        public synchronized void completed(RedisRawResponse result, Throwable e) {
            if(e != null) {
                System.out.println("GOT READ RESPONSE ERROR");
                e.printStackTrace();
            }
            else {
                result.print();
                while ((result =responseReader.readResponse(this)) != null) {
                    result.print();
                }
            }
        }

    }

    private static class WriteHandler implements AsyncHandler<Integer> {

        private AsyncSocketTransport con;

        private FlushHandler flushHandler;

        private boolean writeComplete;

        private boolean isWriteError;

        public WriteHandler(AsyncSocketTransport pcon) {
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

        private WriteHandler writeHandler1;

        public FlushHandler(AsyncSocketTransport pcon, WriteHandler pwriteHandler1) {
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
        RedisResponseReader responseReader = new RedisResponseReader(con);
        ResponseHandler responseHandler = new ResponseHandler(responseReader);
        responseReader.readResponse(responseHandler);
        WriteHandler writeHandler1 = new WriteHandler(con);
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
