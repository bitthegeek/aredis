package org.aredis.cache;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Holds The Load Status of each script. Implemented as a packed boolean array with 1 bit for each
 * Script index.
 *
 * This is configured as a a singleton for each Redis Server in RedisServerWideData and is fetched
 * in the constructor of every AsyncRedisConnection.
 *
 * @author Suresh
 *
 */
class ScriptStatuses {

    private static final Log log = LogFactory.getLog(ScriptStatuses.class);

    private volatile int [] statusFlags = new int[0];

    private long firstStatusUpdateTime;

    public int[] getStatusFlags() {
        return statusFlags;
    }

    public static boolean isLoaded(int [] flags, Script s) {
        int index = s.getIndex();
        int ofs = index >> 5;
        boolean result = false;
        if (ofs < flags.length) {
            result = (flags[ofs] & (1 << (index & 31))) != 0;
        }

        return result;
    }

    public int [] setLoaded(int [] flags, Script s, boolean newStatus) {
        int index = s.getIndex();
        int ofs = index >> 5;
        int statusFlag = 1 << (index & 31);
        boolean status = false;
        if (flags == null) {
            flags = statusFlags;
        }
        if (ofs < flags.length) {
            status = (flags[ofs] & statusFlag) != 0;
        }
        if (status != newStatus) {
            synchronized (this) {
                int [] flags1 = flags;
                flags = statusFlags;
                int oldLen = flags.length;
                int newLen = oldLen;
                if (ofs < newLen) {
                    if (flags != flags1) {
                        status = (flags[ofs] & statusFlag) != 0;
                    }
                } else {
                    newLen = ofs + 1;
                }
                if (status != newStatus) {
                    flags = Arrays.copyOf(flags, newLen);
                    if (newStatus) {
                        flags[ofs] |= statusFlag;
                    } else {
                        flags[ofs] &= ~statusFlag;
                    }
                    if (oldLen == 0) {
                        firstStatusUpdateTime = System.currentTimeMillis();
                    }
                    statusFlags = flags;
                }
            }
        }

        return flags;
    }

    public void clearLoadStatuses() {
        statusFlags = new int[0];
    }

    public synchronized boolean clearLoadStatusesIfLoaded(Script s) {
        boolean status = isLoaded(statusFlags, s);
        if (status) {
            statusFlags = new int[0];
        }

        return status;
    }

    public synchronized boolean clearLoadStatusesIfFirstStatusUpdateBefore(long time) {
        boolean status = false;
        boolean isDebug = log.isDebugEnabled();
        if (firstStatusUpdateTime <= time) {
            if (isDebug) {
                log.debug("CLEARING LOAD STATUSES since firstStatusUpdateTime " + firstStatusUpdateTime + " <= Server Start Time " + time);
            }
            status = true;
            if (statusFlags.length > 0) {
                statusFlags = new int[0];
            }
        } else if (isDebug) {
            log.debug("SKIPPING CLEARING LOAD STATUSES since firstStatusUpdateTime " + firstStatusUpdateTime + " > Server Start Time " + time);
        }

        return status;
    }

    long getFirstStatusUpdateTime() {
        return firstStatusUpdateTime;
    }
}
