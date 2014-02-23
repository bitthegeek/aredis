package org.aredis.util;

import java.util.Arrays;
import java.util.Comparator;


public class SortedArray<T> {

    private volatile Object [] items = new Object[0];

    public int getIndex(T [] itemHolder, Comparator<T> c) {
        Object item = itemHolder[0];
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
                    int i = -arrayIndex - 1;
                    index = itemsArray.length;
                    itemsArray = new Object[index + 1];
                    System.arraycopy(itemsArray1, 0, itemsArray, 0, i);
                    itemsArray[i] = item;
                    System.arraycopy(itemsArray1, i, itemsArray, i + 1, index - i);
                    items = itemsArray;
                }
            }
        }
        if (arrayIndex >= 0) {
            itemHolder[0] = (T) itemsArray[arrayIndex];
        }

        return index;
    }
}
