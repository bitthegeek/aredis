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

import java.io.IOException;

import org.aredis.io.CompressibleByteArrayOutputStream;
import org.aredis.net.ServerInfo;

/**
 * Specifies the serialization and de-serialization of a value when storing to and retrieving an Object from the Redis Server.
 * This is used to marshal and unmarshal the values in a Redis Command. The keys and parameters in the command use a {@link StringHandler}
 * to encode the keys and values in UTF-8.
 * @author Suresh
 *
 */
public interface DataHandler {

    /**
     * Serialze value
     * @param data Data to serialize
     * @param metaData Additional Info to serialize if required by the handler like the Object Class for example.
     * Pass null if the Data Handler does not require addtional info.
     * @param op An output stream which can automatically compress the data in a session if it crosses a configured.
     * Compression Threshold
     * @param serverInfo Redis ServerInfo containing the Redis Server details
     * @throws IOException If there is an error during serialization
     */
    void serialize(Object data, Object metaData, CompressibleByteArrayOutputStream op, ServerInfo serverInfo) throws IOException;

    /***
     * de-serialize value
     * @param metaData  Additional Info to de-serialize if required by the handler
     * @param b Byte Array containing the data from the Redis Server
     * @param offset Starting offset of the data
     * @param len Length of the data in bytes
     * @param serverInfo Redis ServerInfo containing the Redis Server details
     * @return De-serialized Data
     * @throws IOException If there is an error during de-serialization
     */
    Object deserialize(Object metaData, byte [] b, int offset, int len, ServerInfo serverInfo) throws IOException;

}
