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

package org.aredis.cache;

import java.util.ArrayDeque;
import java.util.Deque;

import org.aredis.cache.RedisCommandInfo.ResultType;
import org.aredis.io.ReusableByteArrayOutputStream;
import org.aredis.net.AsyncSocketTransport;
import org.aredis.util.GenUtil;

/**
 * This is a low level redis response parser that is used by aredis. This class is not Thread Safe.
 * @author Suresh
 *
 */
public class RedisResponseReader implements AsyncHandler<Integer> {

    private static enum ResponseState {READING_LINE, READING_EMPTY_LINE, READING_BULK}

    private AsyncSocketTransport con;

    private Deque<RedisRawResponse> mbParents;

    private AsyncHandler<RedisRawResponse> responseHandler;

    private ResponseState responseState;

    private RedisRawResponse response;

    private ReusableByteArrayOutputStream lineBuf = new ReusableByteArrayOutputStream();

    private ReusableByteArrayOutputStream bulkOp = new ReusableByteArrayOutputStream(1);

    private StringBuffer debugBuf;

    /**
     * Creates a RedisResponseReader.
     * @param pcon AsyncSocketTransport to read the response from
     */
    public RedisResponseReader(AsyncSocketTransport pcon) {
        con = pcon;
        mbParents = new ArrayDeque<RedisRawResponse>(2);
    }

    /**
     * Reads the response from the Redis Server. Returns the response immediately if the underlying connection has all the
     * data in its buffer.
     * Otherwise it returns the response asynchronously by calling the passed responseHandler.
     * @param presponseHandler Response Handler to use in case the response is not immediately available
     * @return Response in case the response is immediately available in which case the handler is not called, null otherwise
     */
    public RedisRawResponse readResponse(AsyncHandler<RedisRawResponse> presponseHandler) {
        responseHandler = presponseHandler;
        mbParents.clear();
        lineBuf.reset();
        responseState = ResponseState.READING_LINE;
        response = new RedisRawResponse();
        boolean completed = completed(0, false);
        RedisRawResponse result = null;
        if(completed) {
            result = response;
        }

        return result;
    }

    private void handleError(Throwable e) {
        RedisRawResponse result = mbParents.peekLast();
        if(result == null) {
            result = response;
        }
        result.setError(true);
        responseHandler.completed(result, e);
    }

    private boolean processLine(String s) {
        boolean responseComplete = false;
        ResultType resultType = response.getResultType();
        boolean isDebug = debugBuf != null;

        if(isDebug) {
            GenUtil.addDebug(debugBuf, "Entering processLine line = '" + s + '\'');
            int stackSize = mbParents.size();
            int bulkIndex = - 1;
            if(stackSize > 0) {
                bulkIndex = mbParents.peek().nextResponseIndex;
            }
            GenUtil.addDebug(debugBuf, "responseState = " + responseState + " stackSize = " + stackSize + " bulkIndex = " + bulkIndex + " Bulk Data Len = " + (response.getResult() != null  && response.getResult() instanceof byte [] ? ((byte []) response.getResult()).length : -1));
        }

        if(responseState == ResponseState.READING_EMPTY_LINE) {
            responseState = ResponseState.READING_LINE;
            responseComplete = true;
        }
        else { // responseState is ResponseState.READING_LINE
            int len;
            char ch = s.charAt(0);
            switch(ch) {
            case ':':
                resultType = ResultType.INT;
                response.setResult(s.substring(1));
                break;
            case '+':
                resultType = ResultType.STRING;
                response.setResult(s.substring(1));
                break;
            case '-':
                resultType = ResultType.REDIS_ERROR;
                response.setResult(s.substring(1));
                break;
            case '$':
                resultType = ResultType.BULK;
                break;
            case '*':
                resultType = ResultType.MULTIBULK;
                break;
            }
            response.setResultType(resultType);
            if(resultType == ResultType.BULK) {
                len = Integer.parseInt(s.substring(1));
                byte[] bulkData = null;
                if(len >= 0) {
                    bulkData = new byte[len];
                    if(len > 0) {
                        bulkOp.setBuf(bulkData);
                        responseState = ResponseState.READING_BULK;
                    }
                    else {
                        responseState = ResponseState.READING_EMPTY_LINE;
                    }
                }
                else {
                    responseComplete = true;
                }
                response.setResult(bulkData);
            }
            else if(resultType == ResultType.MULTIBULK) {
                len = Integer.parseInt(s.substring(1));
                RedisRawResponse responses[] = null;
                RedisRawResponse originalResponse = response;
                if(len > 0) {
                    responses = new RedisRawResponse[len];
                    originalResponse.nextResponseIndex = 0;
                    mbParents.push(response);
                    response = new RedisRawResponse();
                    responses[0] = response;
                }
                else {
                    if(len == 0) {
                        responses = new RedisRawResponse[0];
                    }
                    responseComplete = true;
                }
                originalResponse.setResult(responses);
            }
            else {
                responseComplete = true;
            }

            if(isDebug) {
                GenUtil.addDebug(debugBuf, "Got resultType: " + resultType + ", responseState = " + responseState);
            }
        }
        if(isDebug) {
            GenUtil.addDebug(debugBuf, "Got responseComplete = " + responseComplete);
        }
        if(responseComplete) {
            // Check MultiBulk Parents
            RedisRawResponse mbParent = null;
            while((mbParent = mbParents.peek()) != null) {
                RedisRawResponse responses[] = (RedisRawResponse []) mbParent.getResult();
                int nextResponseIndex = ++mbParent.nextResponseIndex;
                if(nextResponseIndex < responses.length) {
                    response = new RedisRawResponse();
                    responses[nextResponseIndex] = response;
                    break;
                }
                response = mbParent;
                mbParents.pop();
            }
            if(mbParent != null) {
                responseComplete = false;
            }
        }
        if(isDebug) {
            GenUtil.addDebug(debugBuf, "Finally responseComplete = " + responseComplete);
        }

        return responseComplete;
    }

    private boolean completed(int count, boolean callbackOnComplete) {
        boolean isComplete = false;
        try {
            boolean continueProcessing = false;
            boolean isDebug = debugBuf != null;
            do {
                continueProcessing = false;
                if(isDebug) {
                    GenUtil.addDebug(debugBuf, "RedisResponseReader: Entering Response Handler, state = " + responseState + " count = " + count);
                }
                if(responseState == ResponseState.READING_LINE || responseState == ResponseState.READING_EMPTY_LINE) {
                    char prevCh = 0, ch = 0;
                    byte [] buf = lineBuf.getBuf();
                    int pos = lineBuf.getCount();
                    if(pos > 1) {
                        prevCh = (char) buf[pos - 2];
                    }
                    if(pos > 0) {
                        ch = (char) buf[pos - 1];
                    }
                    boolean lineComplete;
                    while( !(lineComplete = prevCh == '\r' && ch == '\n')) {
                        if(con.read(lineBuf, 1, this) <= 0) {
                            break;
                        }
                        prevCh = ch;
                        buf = lineBuf.getBuf();
                        pos = lineBuf.getCount();
                        ch = (char) buf[pos - 1];
                    }
                    if(lineComplete) {
                        lineBuf.reset();
                        isComplete = processLine(new String(buf, 0, pos - 2));
                        continueProcessing = !isComplete;
                    }
                }
                else { // responseState == ResponseState.READING_BULK
                    byte[] bulkBuf = (byte[]) response.getResult();
                    int remain;
                    while((remain = bulkBuf.length - bulkOp.getCount()) > 0) {
                        if(con.read(bulkOp, remain, this) <= 0) {
                            break;
                        }
                    }
                    if(remain <= 0) {
                        responseState = ResponseState.READING_EMPTY_LINE;
                        continueProcessing = true;
                    }
                }
                if(continueProcessing) {
                    count = 0;
                }
            } while(continueProcessing);
            if(isComplete && callbackOnComplete) {
                responseHandler.completed(response, null);
            }
        }
        catch(Exception e) {
            handleError(e);
        }

        return isComplete;
    }

    /**
     * This method is an internal method made public as a consequence of implementing AsyncHandler.
     */
    @Override
    public void completed(Integer result, Throwable e) {
        if(result >= 0 && e == null) {
            completed(result, true);
        }
        else {
            handleError(e);
        }
    }

    /**
     * Gets the response which is saved after it is read and parsed.
     * @return Saved Response
     */
    public RedisRawResponse getResponse() {
        return response;
    }
}
