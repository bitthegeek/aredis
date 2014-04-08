package org.aredis.util;

import java.util.Arrays;
import java.util.Comparator;


/**
 * A utility class to hold a unique instance of comparable objects like ServerInfo or Script in an array.
 * Each object is assigned a new index when it is not found which the created object is expected to hold.
 *
 * This enables creation of Array based data structures which can be looked up by the object index instead of a map.
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
