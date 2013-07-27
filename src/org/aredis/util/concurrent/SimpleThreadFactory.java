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

package org.aredis.util.concurrent;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * A utility class which cannot be instantiated but which provides two final instances of ThreadFactorys one for
 * creating daemon threads and one for creating user threads.
 *
 * @author Suresh
 *
 */
public final class SimpleThreadFactory implements ThreadFactory {

    /**
     * ThreadFactory which creates daemon threads.
     */
    public static final SimpleThreadFactory daemonThreadFactory = new SimpleThreadFactory(true);

    /**
     * ThreadFactory which creates user threads.
     */
    public static final SimpleThreadFactory threadFactory = new SimpleThreadFactory(false);

    private ThreadFactory def = Executors.defaultThreadFactory();

    private boolean isDaemon;

    public Thread newThread(Runnable r) {
      Thread t = def.newThread(r);
      t.setDaemon(isDaemon);
      return t;
    }

    private SimpleThreadFactory(boolean pisDaemon) {
        isDaemon = pisDaemon;
    }
}
