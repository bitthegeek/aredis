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

import org.aredis.cache.RedisCommandInfo.ResultType;

/**
 * Data Object representing the Redis Response Read and parsed by {@link RedisResponseReader}.
 * The representation is very similar to Redis's protocol definition.
 * @author Suresh
 *
 */
public class RedisRawResponse {

    private ResultType resultType;

    private Object result;

    private boolean isError;

    int nextResponseIndex;

    /**
     * Gets the Result Type.
     * @return Result Type
     */
    public ResultType getResultType() {
        return resultType;
    }

    /**
     * Sets the result type.
     * @param presultType Result Type
     */
    public void setResultType(ResultType presultType) {
        resultType = presultType;
    }

    /**
     * Gets the result. It is a String if the Result Type is not BULK or MULTIBULK. It is a byte array
     * if the result is of type BULK. In case the Result Type is MULTIBULK the result is an array of
     * RedisRawResponse each of which has the individual items of the MULTIBULK response.
     * @return Redis Response
     */
    public Object getResult() {
        return result;
    }

    /**
     * Sets the result
     * @param presult Redis Response
     */
    public void setResult(Object presult) {
        result = presult;
    }

    /**
     * Returns if there was an error when reading the response.
     * @return true if there was an error, false otherwise
     */
    public boolean isError() {
        return isError;
    }

    /**
     * Sets the error flag to indicate an error while reading the response.
     * @param pisError error flag
     */
    public void setError(boolean pisError) {
        isError = pisError;
    }

    private void printInternal(String indent) {
        int i;
        System.out.print(indent); System.out.println(resultType);
        if(resultType == ResultType.MULTIBULK) {
            RedisRawResponse responses[] = (RedisRawResponse[]) result;
            if(responses == null) {
                System.out.print(indent); System.out.println("(NULL)");
            }
            else if(responses.length == 0) {
                System.out.print(indent); System.out.println("(EMPTY)");
            }
            else {
                for(i = 0; i < responses.length; i++) {
                    responses[i].printInternal(indent + "    ");
                }
            }
        }
        else {
            String s = null;
            if(resultType == ResultType.BULK) {
                byte resultBytes[] = (byte []) result;
                if(resultBytes == null) {
                    s = "(NULL)";
                }
                else if(resultBytes.length == 0) {
                    s = "(EMPTY)";
                }
                else {
                    s = new String(resultBytes);
                }
            }
            else {
                s = (String) result;
            }
            System.out.print(indent); System.out.println(s);
        }
    }

    /**
     * Prints the redis response in a textual representation assuming there is no binary data.
     */
    public void print() {
        printInternal("");
    }

}
