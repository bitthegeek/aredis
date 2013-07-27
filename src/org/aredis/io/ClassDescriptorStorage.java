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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aredis.util.GenUtil;
import org.aredis.util.PeriodicTask;
import org.aredis.util.PeriodicTaskPoller;

/**
 * Defines a persistent ClassDescriptor Store for {@link OptiObjectInputStream} and {@link OptiObjectOutputStream}.
 * The store should be accessible to retrieve the class descriptors and also update it in case the
 * OptiObjectOutputStream encounters a new class.
 * @author Suresh
 *
 */
public abstract class ClassDescriptorStorage {

    private class UpdateTimingsAndCleanupTask implements Runnable {

        @Override
        public void run() {
            try {
                long now = System.currentTimeMillis();
                long lastUseTimeForCleanup = now - GenUtil.MILLIS_IN_A_DAY * cleanupAgeDays;
                boolean success = updateTimingsAndCleanup(useTimeUpdateInterval, lastUseTimeForCleanup);
                if(!success) {
                    ClassDescriptors masterDescriptors = getMasterClassDescriptors(false);
                    if(masterDescriptors != null && masterDescriptors.requiresTimingUpdate(useTimeUpdateInterval)) {
                        // Not making lastCleanupTime volatile. Tired of them.
                        // Manipulate lastCleanupTime on failed update so that it is tried
                        // after 5 minutes instead of 1 hour
                        lastCleanupTime = now - CLEANUP_INTERVAL + 300000;
                    }
                }
            }
            catch(Exception e) {
                log.error("Error During UpdateTimingsAndCleanupTask", e);
            }
        }
    }

    private class CleanupPeriodicTask implements PeriodicTask {

        @Override
        public void doPeriodicTask(long now) {
            try {
                // Restrict activity to once every hour
                if(now - lastCleanupTime > CLEANUP_INTERVAL) {
                    lastCleanupTime = now;
                    ClassDescriptors masterDescriptors = getMasterClassDescriptors(false);
                    if(masterDescriptors != null && masterDescriptors.requiresTimingUpdate(useTimeUpdateInterval)) {
                        new Thread(updateTimingsAndCleanupTask).start();
                    }
                }
            }
            catch(Exception e) {
                log.error("Error Running Cleanup Task", e);
            }
        }

    }

    private static final Log log = LogFactory.getLog(ClassDescriptorStorage.class);

    private static final long CLEANUP_INTERVAL = 3600000;

    /**
     * Gets the class descriptors from the store. If refreshFromStore is false then a local copy if available
     * is returned. If refreshFromStore is true or a local copy is not available then it is fetched from the
     * store. If the class descriptors are not yet stored then a new instance is created and returned.
     * @param refreshFromStore Whether to fetch the Class Descriptors from store even if a local copy is available
     * @return Non null ClassDescriptors
     * @throws IOException In case of error fetching the ClassDescriptors
     */
    public abstract ClassDescriptors getMasterClassDescriptors(boolean refreshFromStore) throws IOException;

    /**
     * Updates the class descriptors based on optimistic locking (Dirty write check) similar to WATCH-MULTI-EXEC or
     * in redis or CAS in memcached. The passed Class Descriptors should be saved if the versionNo of the passed
     * descriptors is 1 greater than that in the store. If the descriptors have not yet been created then the incoming
     * versionNo should be 1. If the update fails then the local ClassDescriptors copy should be replaced with the
     * latest copy from the store. The caller will then retry the operation by using the new ClassDescriptors.
     * @param updatedDescriptors Updated Class Descriptors
     * @return true if the operation succeeds, false if it fails because it has already been updated since the local
     * copy was fetched.
     * @throws IOException In case there is an error updating the ClassDescriptors
     */
    public abstract boolean updateClassDescriptors(ClassDescriptors updatedDescriptors)
            throws IOException;

    private long useTimeUpdateInterval;

    private int cleanupAgeDays;

    private long lastCleanupTime;

    private UpdateTimingsAndCleanupTask updateTimingsAndCleanupTask = new UpdateTimingsAndCleanupTask();

    /**
     * Creates a Class Descriptor Storage.
     */
    public ClassDescriptorStorage() {
        // First cleanup is 15 seconds after start
        lastCleanupTime = System.currentTimeMillis() - CLEANUP_INTERVAL + 15000;
        useTimeUpdateInterval = GenUtil.MILLIS_IN_A_DAY;
        cleanupAgeDays = 36500; // 100 Years
        PeriodicTaskPoller periodicTaskPoller = PeriodicTaskPoller.getInstance();
        CleanupPeriodicTask periodicTask = new CleanupPeriodicTask();
        periodicTaskPoller.addTask(periodicTask);
    }

    /**
     * Updates the last use times of the descriptors and removes old descriptors not used since the passed
     * lastUseTimeForCleanup. It uses the requiresTimingUpdate and removeOldDescriptors methods of ClassDescriptors.
     * This method is invoked via a periodic task initialized in the constructor. The cleanup feature has not been
     * tested, so 100 years ago is the default lastUseTimeForCleanup used.
     * @param updateInterval Specifies the time interval required between the current lastUseTime and lastUseTime when
     * the descriptors were fetched from the store so that the descriptors are updated to update lastUseTime
     * @param lastUseTimeForCleanup Last Use Time in milliseconds time. Descriptors not used since this time are removed
     * from the store by setting the corresponding entry in the array to null
     * @return true if the update was successful, false otherwise
     * @throws IOException
     */
    public boolean updateTimingsAndCleanup(long updateInterval, long lastUseTimeForCleanup) throws IOException {
        boolean updated = false;
        ClassDescriptors masterDescriptors = getMasterClassDescriptors(false);
        if(masterDescriptors != null && masterDescriptors.requiresTimingUpdate(updateInterval)) {
            try {
                ClassDescriptors clonedDescriptor = masterDescriptors.clone(true);
                clonedDescriptor.removeOldDescriptors(lastUseTimeForCleanup);
                updated = updateClassDescriptors(clonedDescriptor);
            } catch (CloneNotSupportedException e) {
                log.error("Exception while cloning", e);
            }
        }

        return updated;
    }

    /**
     * Getter for useTimeUpdateInterval. This is used in the periodic task which calls updateTimingsAndClenaup.
     * The default is 1 day (Milliseconds in a day).
     * @return the useTimeUpdateInterval
     */
    public long getUseTimeUpdateInterval() {
        return useTimeUpdateInterval;
    }

    /**
     * Setter for useTimeUpdateInterval.
     * @param puseTimeUpdateInterval the useTimeUpdateInterval to set
     */
    public void setUseTimeUpdateInterval(long puseTimeUpdateInterval) {
        useTimeUpdateInterval = puseTimeUpdateInterval;
    }

    /**
     * Getter for cleanupAgeDays. This is used to calculate the lastUseTimeForCleanup in the periodic task which
     * calls updateTimingsAndClenaup. The default for this is 36500 (100 years).
     * @return the cleanupAgeDays
     */
    public int getCleanupAgeDays() {
        return cleanupAgeDays;
    }

    /**
     * Setter for cleanupAgeDays.
     * @param pcleanupAgeDays the cleanupAgeDays to set
     */
    public void setCleanupAgeDays(int pcleanupAgeDays) {
        cleanupAgeDays = pcleanupAgeDays;
    }
}
