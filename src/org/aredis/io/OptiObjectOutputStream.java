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
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;

/**
 * <p>
 * An ObjectOutputStream which optimizes storage by using a common ClassDescriptorStorage for maintaining the
 * Class Descriptors for the class used in serialization. An index pointing to the central array of descriptors
 * in the ClassDescriptorStorage is what is present in the stream.
 * </p>
 *
 * <p>
 * The ClassDescriptorStorage is required to be up and working whenever a new class is encountered that is not present
 * in the Local copy of the ClassDescriptors when writeObject method is called.
 * If syncToStore field is set to true the ClassDescriptor storage is automatically updated with the local
 * ClassDescriptors in that case. If the store attempt fails because the ClassDescriptors was updated in the store
 * since it was read the operation is attempted again. If syncToStore is set to false then it is the responsibility
 * of the user of this class to update the store with the new Class Descriptors available via the getUpdatedDescriptors
 * method.
 * If the update fails then the writeObject should be called again. Setting syncToStorage false is better since it
 * makes fewer interactions with the store. However the user has to update the store when new Classes are encountered
 * and the writeObject should be called again if the update fails. For retrying the writeObject the user has to either
 * use a local Byte Array stream or use the mark/reset feature on a BufferedOutputStream. aredis uses false for syncToStorage.
 * </p>
 *
 * <p>
 * This class is used by the OPTI_JAVA_HANDLER or aredis.
 * </p>
 *
 * @author Suresh
 *
 */
public class OptiObjectOutputStream extends ObjectOutputStream {

    private static final int SEVEN_BITS = 127;

    private static final int EIGHTH_BIT = 128;

    private ClassDescriptorStorage descriptorsStorage;

    private ClassDescriptors masterDescriptors;

    private ClassDescriptors [] updatedDescriptorsArr;

    private boolean syncToStorage;

    private int [] digits;

    private ClassDescriptorOutputStream[] tmpOpHolder;

    /**
     * Creates an OptiObjectOutputStream. When syncToStorage is set to true
     * @param out Underlying OutputStream
     * @param pdescriptorsStorage Whether to sync the local class descriptors to store whenever a new class is
     * encountered in writeObject. This is simpler to use.
     * @param psyncToStorage Class Descriptors store to use
     * @throws IOException In case of IO error
     */
    public OptiObjectOutputStream(OutputStream out, ClassDescriptorStorage pdescriptorsStorage, boolean psyncToStorage) throws IOException {
        super(out);
        descriptorsStorage = pdescriptorsStorage;
        updatedDescriptorsArr = new ClassDescriptors[1];
        masterDescriptors = descriptorsStorage.getMasterClassDescriptors(false);
        if(masterDescriptors == null) {
            masterDescriptors = descriptorsStorage.getMasterClassDescriptors(true);
            if(masterDescriptors == null) {
                masterDescriptors = new ClassDescriptors();
            }
        }
        syncToStorage = psyncToStorage;
        digits = new int[5];
        tmpOpHolder = new ClassDescriptorOutputStream[1];
    }

    /**
     * Creates an OptiObjectOutputStream with syncToStorate as true.
     * @param out Underlying OutputStream
     * @throws IOException In case of IO error
     */
    public OptiObjectOutputStream(OutputStream out, ClassDescriptorStorage pdescriptorsStorage) throws IOException {
        this(out, pdescriptorsStorage, true);
    }

    private void writeEncodedInteger(int index) throws IOException {
        int i, n = index;
        digits[0] = n & SEVEN_BITS;
        i = 1;
        while( (n = n >> 7) > 0) {
            digits[i] = EIGHTH_BIT | (n & SEVEN_BITS);
            i++;
        }
        do {
            i--;
            write(digits[i]);
        } while(i > 0);
    }

    @Override
    protected void writeClassDescriptor(ObjectStreamClass desc)
            throws IOException {
        int maxStorageTries = 1000;
        ClassDescriptors currentDescriptors = updatedDescriptorsArr[0];
        ClassDescriptors descriptors = currentDescriptors;
        if(descriptors == null) {
            descriptors = masterDescriptors;
        }
        int index;
        boolean retry = false;
        do {
            retry = false;
            index = descriptors.getDescriptorIndex(desc, updatedDescriptorsArr, tmpOpHolder);
            if(currentDescriptors == null) {
                ClassDescriptors newDescriptors = updatedDescriptorsArr[0];
                if(newDescriptors != null) {
                    ClassDescriptors latestMasterDescriptors = descriptorsStorage.getMasterClassDescriptors(false);
                    if(latestMasterDescriptors != masterDescriptors) {
                        // Retry if the masterDescriptors have changed
                        masterDescriptors = latestMasterDescriptors;
                        updatedDescriptorsArr[0] = null;
                        retry = true;
                    }
                    else if(syncToStorage) {
                        if(maxStorageTries == 0) {
                            throw new IOException("Failed to store update class Descriptor after 1000 tries");
                        }
                        updatedDescriptorsArr[0] = null;
                        maxStorageTries--;
                        if(!descriptorsStorage.updateClassDescriptors(newDescriptors)) {
                            retry = true;
                        }
                        masterDescriptors = descriptorsStorage.getMasterClassDescriptors(false);
                    }
                    descriptors = masterDescriptors;
                }
            }
        } while(retry);
        writeEncodedInteger(index);
    }

    /**
     * Gets the updated descriptors. If this returns a non null value, the user should try to save the returned
     * ClassDescriptors to the ClassDescriptorStorage. If the storage fails the writeObject should be tried again.
     * This method always returns false if syncToStorage is set to true.
     *
     * @return Updated ClassDescriptors
     */
    public ClassDescriptors getUpdatedDescriptors() {
        return updatedDescriptorsArr[0];
    }

    /**
     * Checks whether syncToStorage is enabled.
     * @return syncToStorage
     */
    public boolean isSyncToStorage() {
        return syncToStorage;
    }
}
