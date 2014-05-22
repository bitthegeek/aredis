/*
 * Copyright (C) 2014 Suresh Mahalingam.  All rights reserved.
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

import java.security.NoSuchAlgorithmException;

import org.aredis.util.GenUtil;
import org.aredis.util.SortedArray;
import org.aredis.util.SortedArray.IndexUpdater;

/**
 * <p>
 * A simple immutable holder class to hold a Redis Lua Script and its sha1 digest.
 * A script Object is meant for scripts that are going to be used repeatedly which is normally
 * the case. Ideally scripts should be static variables or Singleton POJOs containing scripts if
 * loaded from a file on jvm start. A Script object is created using the static getInstance method.
 * </p>
 *
 * <p>
 * A script Object should be used with a {@link RedisCommand#EVALCHECK} pseudo command.
 * There is a flag for each Script maintained against each Redis Server. When an {@link AsyncRedisConnection}
 * processes an EVALCHECK command for the first time when the flag for the script is not set against the
 * server, it checks if the script is present on the Redis Server by using the SCRIPT EXISTS command on a
 * separate Redis Connection. If SCRIPT EXISTS returns 0 then the script is loaded using the SCRIPT LOAD command.
 * Then the flag for the script is set to indicate that the script is verified/loaded on the server after which
 * an EVALSHA is sent with the script digest as the first parameter. Subsequent EVALCHECK calls for the same
 * script on the same server will straight away translate to EVALSHA since the flag for the script is set
 * against the server.
 * </p>
 *
 * <p>
 * A script Object can also be used instead of a String to specify the script for the
 * EVAL, EVALSHA, SCRIPT LOAD and SCRIPT EXISTS commands though you do not need these commands if you use the
 * EVALCHECK pseudo-command. The advantage of using a Script Object for these commands in case you
 * have a need to use them is that the script is marked as loaded for the server once the command
 * completes successfully which can be used by the EVALCHECK pseudo command.
 * </p>
 *
 * <p>
 * This class also includes Script constants containing Scripts used in aredis.
 * </p>
 *
 * <p>
 * Also note that EVALCHECK commands will start failing if a SCRIPT FLUSH command is executed on the
 * Redis server since the jvm does not know that scripts have been cleared. In such cases the script
 * all script statuses are cleared on detecting the failure and the subsequent EVALCHECK commands will go through.
 * </p>
 *
 * @author Suresh
 *
 */
public class Script implements Comparable<Script> {

    private static final SortedArray<Script> scriptsArray = new SortedArray<Script>();

    /**
     * Though Redis provides a SETNX to atomically add a key if it does not exist, it does not take an expiry time. This Redis Lua script provides an equivalent of memcached add command.
     * It takes a key, an expiry time and a value and atomically sets it with the expiry time if it does not exist. This is used for the lock functionality as well to acquire a
     * distributed lock on a cache server with an expiry time. The lock is considered acquired if the script returns 1.
     *
     * @deprecated Since version 2.6.12 Redis has an enhanced SET command which has an NX and expiry option. So this script is not required.
     */
    public static final Script ADD_SCRIPT = Script.getInstance("local e = 1 - redis.call('exists',KEYS[1])\nif e == 1 then\n    redis.call('setex',KEYS[1],ARGV[1],ARGV[2])\nend\nreturn e");

	private String script;

    private String sha1sum;

    private int index;

    /**
     * This static method must be used to create a Script Object.
     * @param s Redis script
     * @return A script object containing the Redis script and its SHA-1 digest
     */
    public static Script getInstance(String s) {
        final Script searchScr = new Script(s);
        Script scr = scriptsArray.findItem(searchScr, null, new IndexUpdater() {
            @Override
            public void updateIndex(int index) {
                searchScr.index = index;
            }
        });

        return scr;
    }

    /**
     * Creates a Script Object.
     *
     * @param pscript Redis Script
     * @param pindex Correct index of the script from scriptsArray
     */
    private Script(String pscript) {
        script = pscript;
        try {
			sha1sum = GenUtil.digest(script, "SHA-1");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
    }

    @Override
	public int compareTo(Script o) {
		int result = sha1sum.compareTo(o.sha1sum);
		if (result == 0) {
			result = script.compareTo(o.script);
		}

		return result;
	}

	@Override
	public boolean equals(Object o) {
		boolean result = false;
		if (this == o) {
			result = true;
		} else if (o instanceof Script) {
			result = compareTo((Script) o) == 0;
		}

		return result;
	}

	@Override
	public int hashCode() {
		return sha1sum.hashCode();
	}

    /**
     * Gets the script.
     * @return Redis script
     */
    public String getScript() {
        return script;
    }

    /**
     * Gets the SHA-1 digest of the script.
     * @return SHA-1 digest
     */
    public String getSha1sum() {
        return sha1sum;
    }

    /**
     * Gets the identifying index of the script. Indexes are sequential starting with 0.
     *
     * @return Script Index
     */
    public int getIndex() {
        return index;
    }
}