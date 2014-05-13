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

package org.aredis.util;

import java.util.Arrays;
import java.util.Comparator;


/**
 * <p>
 * A utility class to hold a unique instance of comparable objects like ServerInfo or Script in an array.
 * Each object is assigned a new index when it is not found which the created object is expected to hold.
 * </p>
 *
 * <p>
 * This enables creation of Array based data structures which can be looked up by the object index instead of a map.
 * </p>
 *
 * @author suresh
 *
 * @param <T>
 */
public class SortedArray<T> {

    public static interface IndexUpdater {
        void updateIndex(int index);
    }
    private volatile Object [] items = new Object[0];

    /**
     * Finds an item or creates it.
     * @param item item to find
     * @param c Comparator to use to search sorted array
     * @param indexUpdater Index Updater to use to update new index in case item was not found
     * @return An existing item if found or the passed item if it was created
     */
    public T findItem(T item, Comparator<T> c, IndexUpdater indexUpdater) {
        T result = null;
        Object[] itemsArray = items;
        int arrayIndex;
        if (c != null) {
            arrayIndex = Arrays.binarySearch(itemsArray, item, (Comparator) c);
        } else {
            arrayIndex = Arrays.binarySearch(itemsArray, item);
        }
        int index = -1;
        if (arrayIndex < 0) {
            synchronized (this) {
                Object [] itemsArray1 = items;
                if (itemsArray1 != itemsArray) {
                    itemsArray = itemsArray1;
                    if (c != null) {
                        arrayIndex = Arrays.binarySearch(itemsArray, item, (Comparator) c);
                    } else {
                        arrayIndex = Arrays.binarySearch(itemsArray, item);
                    }
                }
                if (arrayIndex < 0) {
                    index = itemsArray.length;
                    itemsArray = new Object[index + 1];
                    int i = -arrayIndex - 1;
                    System.arraycopy(itemsArray1, 0, itemsArray, 0, i);
                    itemsArray[i] = item;
                    System.arraycopy(itemsArray1, i, itemsArray, i + 1, index - i);
                    if (indexUpdater != null) {
                        indexUpdater.updateIndex(index);
                    }
                    items = itemsArray;
                    result = item;
                }
            }
        }
        if (arrayIndex >= 0) {
            result = (T) itemsArray[arrayIndex];
        }

        return result;
    }
}
