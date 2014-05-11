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

package org.aredis.cache;

import org.aredis.cache.RedisCommandInfo.ResultType;

/**
 * A Holder for Result Type that is used to specify Individual Result Types recursively in case the
 * Result is of Type MULTIBULK.
 *
 * @author suresh
 */
public class ResultTypeInfo {

    private ResultType resultType;

    private ResultTypeInfo [] mbResultTypes;

    /**
     * Constructor.
     * @param presultType Result Type
     * @param pmbResultTypes Individual Result Types in case result is of Type MULTIBULK. Should be NULL otherwise.
     */
    public ResultTypeInfo(ResultType presultType, ResultTypeInfo [] pmbResultTypes) {
        resultType = presultType;
        mbResultTypes = pmbResultTypes;
    }

    /**
     * Gets Result Type.
     * @return Result Type
     */
    public ResultType getResultType() {
        return resultType;
    }

    /**
     * Gets Individual Result Types in case Result Type is MULTIBULK.
     * @return Individual Result Types of a MULTIBULK response
     */
    public ResultTypeInfo[] getMbResultTypes() {
        return mbResultTypes;
    }
}
