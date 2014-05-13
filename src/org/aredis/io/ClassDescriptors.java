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
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * Holds Class Descriptors in an internal array.
 * </p>
 *
 * <p>
 * This is an important class for OPTI_JAVA_HANDLER. However this and the other io classes are not tied
 * to any aredis specific stuff. Also this is not to be used directly but is used by ClassDescriptorStorage,
 * {@link OptiObjectInputStream} and {@link OptiObjectOutputStream}.
 * </p>
 *
 * <p>
 * This is for maintaining a central array of Class Descriptors used in Object Serialization and de-serialization.
 * The class descriptors in the serialized data is only an index into this array.
 * </p>
 *
 * <p>
 * The array is stored and retrieved by an implementation of {@link ClassDescriptorStorage}.
 * </p>
 *
 * <p>
 * Whenever ClassDescriptors is created or fetched from the store it is read only. It can be made writable
 * by cloning it if a new class descriptor is to be added. Then a save attempt is to be made via ClassDescriptorStorage
 * which should be based on optimistic locking (Dirty Check) using the versionNo. If the save fails a retry should be done.
 * </p>
 * @author Suresh
 *
 */
public class ClassDescriptors extends ClassDescriptorsInfo {

    private static final Log log = LogFactory.getLog(ClassDescriptorsInfo.class);

    /**
     * This holds a single Class Descriptor Info.
     * @author Suresh
     *
     */
    private static class DescriptorInfo implements Serializable {
        private static final long serialVersionUID = 8939067843563768021L;

        private transient int index;

        private ObjectStreamClass objectStreamClass;

        private long createTime;

        private long lastUseTime;

        private transient long lastUseTimeFromRepo;

        private transient volatile ObjectStreamClass [] writtenDescriptors;

        public DescriptorInfo(ObjectStreamClass desc) {
            createTime = System.currentTimeMillis();
            lastUseTime = createTime;
            lastUseTimeFromRepo = createTime;
            if(desc != null) {
                objectStreamClass = desc;
                writtenDescriptors = new ObjectStreamClass[1];
                writtenDescriptors[0] = desc;
            }
            else {
                writtenDescriptors = new ObjectStreamClass[0];
            }
        }

        public boolean isMatches(ObjectStreamClass desc, ClassDescriptorOutputStream tmpOpHolder[]) throws IOException {
            int i;
            boolean matches = false;
            ObjectStreamClass[] wd = writtenDescriptors;
            for(i = wd.length; i > 0; ) {
                i--;
                if(desc == wd[i]) {
                    matches = true;
                    break;
                }
            }
            if(!matches) {
                if(objectStreamClass != null && desc != null) {
                    ClassDescriptorOutputStream tmpOp = null;
                    if(tmpOpHolder != null) {
                        tmpOp = tmpOpHolder[0];
                    }
                    if(tmpOp == null) {
                        tmpOp = new ClassDescriptorOutputStream(new ReusableByteArrayOutputStream());
                        if(tmpOpHolder != null) {
                            tmpOpHolder[0] = tmpOp;
                        }
                    }
                    byte[] serDesc1 = tmpOp.writeDescriptor(objectStreamClass);
                    byte[] serDesc2 = tmpOp.writeDescriptor(desc);
                    if(Arrays.equals(serDesc1, serDesc2)) {
                        matches = true;
                        int to = wd.length + 1;
                        i = 0;
                        if(to > MAX_WRITTEN_DESCRIPTORS_PER_CLASS) {
                            i = to - MAX_WRITTEN_DESCRIPTORS_PER_CLASS;
                        }
                        ObjectStreamClass[] wd1 = Arrays.copyOfRange(wd, i, to);
                        wd1[wd1.length - 1] = desc;
                        writtenDescriptors = wd1;
                    }
                }
                else if(objectStreamClass == null && desc == null) {
                    matches = true;
                }
            }

            return matches;
        }

        private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
            ois.defaultReadObject();
            writtenDescriptors = new ObjectStreamClass[0];
            lastUseTimeFromRepo = lastUseTime;
        }

        public long getCreateTime() {
            return createTime;
        }

        public long getLastUseTime() {
            return lastUseTime;
        }

        public ObjectStreamClass[] getWrittenDescriptors() {
            return writtenDescriptors;
        }
    }

    private static final long serialVersionUID = -2313677405787763136L;

    // Usually The same Object Stream class is repeated for the Same class
    // A max of 3 is stored for matching the reference in case you get different
    // Stream class Objects in say multiple Class Loaders
    private static final int MAX_WRITTEN_DESCRIPTORS_PER_CLASS = 3;

    private transient Map<String, DescriptorInfo []> descriptorMap;

    private DescriptorInfo [] descriptors;

    private transient boolean isWritable;

    /**
     * Creates a read only ClassDescriptors.
     */
    public ClassDescriptors() {
        descriptors = new DescriptorInfo[0];
        descriptorMap = new HashMap<String, DescriptorInfo[]>();
    }

    private void readObject(ObjectInputStream ois)
        throws ClassNotFoundException, IOException {
      ois.defaultReadObject();
      int i = descriptors.length, len;
      descriptorMap = new HashMap<String, DescriptorInfo[]>();
      for(i = 0; i < descriptors.length; i++) {
          DescriptorInfo descInfo = descriptors[i];
          if(descInfo != null) {
              descInfo.index = i;
              ObjectStreamClass descriptor = descInfo.objectStreamClass;
              String className = descriptor.getName();
              len = 0;
              DescriptorInfo matchingInfos[] = descriptorMap.get(className);
              if(matchingInfos != null) {
                  len = matchingInfos.length;
              }
              DescriptorInfo [] newInfos = new DescriptorInfo[len + 1];
              if(len > 0) {
                  System.arraycopy(matchingInfos, 0, newInfos, 0, len);
              }
              newInfos[len] = descriptors[i];
              descriptorMap.put(className, newInfos);
          }
      }
    }

    /**
     * Clones the current descriptor.
     * @param pisWritable Whether new descriptor should be writable. The version number of the clone is incremented
     * if this is not writable and the cloned one is writable.
     * @return Cloned Object
     * @throws CloneNotSupportedException
     */
    public ClassDescriptors clone(boolean pisWritable) throws CloneNotSupportedException {
        ClassDescriptors copy =  (ClassDescriptors) super.clone();
        copy.descriptors = Arrays.copyOf(descriptors, descriptors.length);
        copy.descriptorMap = new HashMap<String, ClassDescriptors.DescriptorInfo[]>(descriptorMap);
        if(!isWritable && pisWritable) {
            copy.versionNo++;
        }
        copy.isWritable = pisWritable;
        return copy;
    }

    /* TODO: Delete
    public void updateRepoLastUseTimes() {
        int i;
        for(i = 0; i < descriptors.length; i++) {
            DescriptorInfo d = descriptors[i];
            if(d != null) {
                d.lastUseTimeFromRepo = d.lastUseTime;
            }
        }
    }
    */

    /**
     * Clones the current descriptor. Equivalent to clone(true).
     */
    @Override
    public ClassDescriptors clone() throws CloneNotSupportedException {
        return clone(true);
    }

    /**
     * Creates an info Object containing the versionNo. This is currently not used.
     * @return An Info Object
     */
    public ClassDescriptorsInfo createInfo() {
        ClassDescriptorsInfo info = new ClassDescriptorsInfo(specVersion);
        info.versionNo = versionNo;
        return info;
    }

    /**
     * Copies meta info like written class descriptors and lastUseTime to another ClassDescriptors.
     * This is used when refreshing the jvm copy of ClassDescriptors from the store. The meta info
     * from the current one is copied to the new one got from store.
     * @param cld Descriptors Object to copy meta info to
     */
    public void copyMetaInfoTo(ClassDescriptors cld) {
        int i, len = cld.descriptors.length;
        if(len > descriptors.length) {
            len = descriptors.length;
        }
        DescriptorInfo cldDescriptors[] = cld.descriptors;
        for(i = 0; i < len; i++) {
            DescriptorInfo fromDesc = descriptors[i];
            DescriptorInfo toDesc = cldDescriptors[i];
            if(fromDesc != null && toDesc != null) {
                toDesc.writtenDescriptors = fromDesc.writtenDescriptors;
                if(fromDesc.lastUseTime > toDesc.lastUseTime) {
                    toDesc.lastUseTime = fromDesc.lastUseTime;
                }
            }
        }
    }

    /**
     * Gets the ObjectStreamClass at the given index.
     * @param index index
     * @return Class Descriptor at given index
     */
    public ObjectStreamClass getDescriptor(int index) {
        ObjectStreamClass desc = null;
        DescriptorInfo descInfo = descriptors[index];
        if(descInfo != null) {
            descInfo.lastUseTime = System.currentTimeMillis();
            desc = descInfo.objectStreamClass;
        }

        return desc;
    }

    /**
     * Gets the descriptor Index corresponding to an ObjectStreamClass.
     * @param desc Descriptor
     * @param updatedDescriptors Updated Descriptors in case current one is not writable
     * @param tmpOpHolder Tmp class descriptor Output Stream. If tmpOpHolder[0] is null one is created and set.
     * @return Class Descriptor Index
     * @throws IOException In case of Serialization errors
     */
    int getDescriptorIndex(ObjectStreamClass desc, ClassDescriptors [] updatedDescriptors, ClassDescriptorOutputStream[] tmpOpHolder) throws IOException {
        int i, descIndex = -1;
        String className = desc.getName();
        DescriptorInfo[] descInfos = descriptorMap.get(className);
        DescriptorInfo descInfo = null;
        if(descInfos != null) {
            for(i = descInfos.length; i > 0;) {
                i--;
                descInfo = descInfos[i];
                if(descInfo.isMatches(desc, tmpOpHolder)) {
                    descIndex = descInfo.index;
                }
            }
        }
        if(descIndex < 0) {
            descIndex = descriptors.length;
            DescriptorInfo [] newDescriptors = Arrays.copyOf(descriptors, descIndex + 1);
            descInfo = new DescriptorInfo(desc);
            descInfo.index = descIndex;
            newDescriptors[descIndex] = descInfo;
            if(descInfos == null) {
                descInfos = new DescriptorInfo[1];
            }
            else{
                descInfos = Arrays.copyOf(descInfos, descInfos.length + 1);
            }
            descInfos[descInfos.length - 1] = descInfo;
            if(isWritable) {
                descriptors = newDescriptors;
                descriptorMap.put(className, descInfos);
            }
            else {
                ClassDescriptors copyOfThis = null;
                try {
                    copyOfThis = this.clone();
                    copyOfThis.descriptors = newDescriptors;
                    copyOfThis.descriptorMap.put(className, descInfos);
                } catch (CloneNotSupportedException e) {
                    log.error("Unexpected Exception", e);
                }
                updatedDescriptors[0] = copyOfThis;
            }
        }
        else {
            descInfo.lastUseTime = System.currentTimeMillis();
        }

        return descIndex;
    }

    /**
     * Removes old descriptors not used since the passed lastUseTime. This gets called via a cron but the remove
     * feature is not used as it involves removing ClassDescriptors and therefore requires some basic testing.
     * Currently lastUseTime is passed as 100 years ago by the cron.
     * @param lastUseTime Last Use Milliseconds time
     * @return Number of descriptors removed
     */
    public int removeOldDescriptors(long lastUseTime) {
        int i, count = 0;
        if(!isWritable) {
            throw new IllegalStateException("Read Only Class Descriptor Cannot be modified");
        }
        for(i = 0; i < descriptors.length; i ++) {
            DescriptorInfo d = descriptors[i];
            if(d != null) {
                if(d.lastUseTime <= lastUseTime) {
                    descriptors[i] = null;
                    d = null;
                    count++;
                }
            }
        }

        return count;
    }

    /**
     * Checks whether the use time fetched from store and current use time differs by more than the given
     * updateInterval for any of the descriptors. A cron calls this with a day as the interval.
     * @param updateInterval Update Interval in milliseconds
     * @return true if an update of the current descriptors to store for storing the last use times is required
     * as per the passed update Interval
     */
    public boolean requiresTimingUpdate(long updateInterval) {
        boolean requiresTimingUpdate = false;
        int i;
        for(i = 0; i < descriptors.length; i ++) {
            DescriptorInfo d = descriptors[i];
            if(d != null) {
                if(d.lastUseTime - d.lastUseTimeFromRepo >= updateInterval) {
                    requiresTimingUpdate = true;
                }
            }
        }

        return requiresTimingUpdate;
    }

    /**
     * Returns if this ClassDescriptors is writable.
     * @return isWritable flag
     */
    public boolean isWritable() {
        return isWritable;
    }

    /**
     * Returns a copy of the Descriptor array. Made the earlier public method private. But this
     * method is not used.
     * @return Array of descriptors
     */
    private DescriptorInfo[] getDescriptors() {
        DescriptorInfo[] copy = descriptors;
        if(copy != null) {
            copy = Arrays.copyOf(copy, copy.length);
        }

        return copy ;
    }

}
