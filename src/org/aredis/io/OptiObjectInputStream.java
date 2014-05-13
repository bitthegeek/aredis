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

package org.aredis.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * <p>
 * An ObjectInputStream which optimizes storage by using a common ClassDescriptorStorage for maintaining the
 * Class Descriptors for the class used in serialization. An index pointing to the central array of descriptors
 * in the ClassDescriptorStorage is what is present in the stream.
 * </p>
 *
 * <p>
 * The ClassDescriptorStorage is required to be up and working when used for the first time and whenever a new
 * ClassDescriptor index not present in the ClassDescriptors maintained by this class is encountered.
 * The Local copy of the ClassDescriptors are automatically refreshed from the store in that case.
 * </p>
 *
 * <p>
 * This class is used by the OPTI_JAVA_HANDLER or aredis.
 * </p>
 *
 * @author Suresh
 *
 */
public class OptiObjectInputStream extends ObjectInputStream {

    private static final int SEVEN_BITS = 127;

    private static final int EIGHTH_BIT = 128;

    private ClassDescriptorStorage descriptorsStorage;

    private ClassDescriptors masterDescriptors;

    /**
     * Creates an OptiObjectInputStream.
     * @param in Underlying InputStream
     * @param pdescriptorsStorage Class Descriptors store to use
     * @throws IOException In case of IO error
     */
    public OptiObjectInputStream(InputStream in, ClassDescriptorStorage pdescriptorsStorage) throws IOException {
        super(in);
        descriptorsStorage = pdescriptorsStorage;
        masterDescriptors = descriptorsStorage.getMasterClassDescriptors(false);
    }

    private int readEncodedInteger() throws IOException {
        int i, n, nextByte, eighthBitStatus;
        i = 0;
        n = 0;
        do {
            nextByte = read();
            if(nextByte < 0) {
                throw new IOException("Unexpected end of input");
            }
            n = (n << 7) + (nextByte & SEVEN_BITS);
            eighthBitStatus = nextByte & EIGHTH_BIT;
            i++;
        } while(i < 5 && eighthBitStatus > 0);
        if(eighthBitStatus > 0) {
            throw new IOException("Last Byte: " + nextByte + " > 127");
        }

        return n;

    }

    private ObjectStreamClass readClassDescriptorInternal(int index) throws IOException, SecurityException, ClassNotFoundException  {
        ObjectStreamClass descriptors = null;
        if(masterDescriptors != null) {
            descriptors = masterDescriptors.getDescriptor(index);
        }

        return descriptors;
    }

    @Override
    protected ObjectStreamClass readClassDescriptor() throws IOException,
            ClassNotFoundException {
        int index = readEncodedInteger();
        ObjectStreamClass descriptors = readClassDescriptorInternal(index);
        if(descriptors == null) {
            masterDescriptors = descriptorsStorage.getMasterClassDescriptors(true);
            descriptors = readClassDescriptorInternal(index);
        }

        return descriptors;
    }

}
