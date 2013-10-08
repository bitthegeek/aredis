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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.BlockingDeque;
import org.aredis.util.pool.AsyncObjectPool;


/**
 * A simple array based synchronized stack implementing a subset of methods in {@link BlockingDeque}.
 * This is for use in {@link AsyncObjectPool} and should be faster than LinkedBlockingDeque in size checks and
 * dequeuing on an empty Q which is expected in a busy pool.
 *
 * @author suresh
 *
 */
public class SynchronizedStack<E> implements java.io.Serializable {

    private static final long serialVersionUID = 5111675042868430124L;

    private Object [] items;

    private volatile int count;

    /**
     * Creates a Synchronized Stack.
     *
     * @param capacity Max elements in the stack
     */
    public SynchronizedStack(int capacity) {
        items = new Object[capacity];
    }

    /**
     * @see BlockingDeque#offerFirst(Object)
     */
    public synchronized boolean offerFirst(E item) {
        boolean success = false;
        if (item == null) throw new NullPointerException();
        if(count < items.length) {
            items[count++] = item;
            success = true;
        }

        return success;
    }

    /**
     * @see BlockingDeque#pollFirst()
     */
    public E pollFirst() {
        E item = null;
        if(count > 0) {
            synchronized(this) {
                if(count > 0) {
                    item = (E) items[--count];
                }
            }
        }

        return item;
    }

    /**
     * @see BlockingDeque#drainTo(Collection, int)
     */
    public synchronized int drainTo(Collection<? super E> c, int maxElements) {
        int numItems = count;
        int n = Math.min(maxElements, numItems);
        if(n > 0) {
            int i = numItems;
            numItems -= n;
            count = numItems;
            do {
                c.add((E) items[--i]);
            } while(i > numItems);
        }

        return n;
    }

    /**
     * @see BlockingDeque#drainTo(Collection)
     */
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    /**
     * @see BlockingDeque#size()
     */
    public int size() {
        return count;
    }

    /**
     * Gets max capacity of this stack was created with which is the length of the underlying array.
     * @return capcity of the stack
     */
    public synchronized int getCapacity() {
        return items.length;
    }

    /**
     * Sets the capacity of the stack to the new value if the current count is not greater than the new capacity.
     *
     * @param newCapacity New Capacity
     * @return true if successful in updating the capacity, false if capacity is unchanged
     */
    public synchronized boolean setCapacity(int newCapacity) {
        boolean success = false;
        if(count <= newCapacity) {
            items = Arrays.copyOf(items, newCapacity);
            success = true;
        }

        return success;
    }
}
