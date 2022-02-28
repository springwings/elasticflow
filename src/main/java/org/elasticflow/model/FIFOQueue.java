package org.elasticflow.model;

import java.util.LinkedList;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-22 09:08
 */
public class FIFOQueue<T> extends LinkedList<T>{
	
	private static final long serialVersionUID = 6002229964464186219L;
	
	private int maxSize = Integer.MAX_VALUE;
	
    private final Object locker = new Object();
 
    public FIFOQueue() {
        super();
    }
 
    public FIFOQueue(int maxSize) {
        super();
        this.maxSize = maxSize;
    }
 
    public T addLastSafe(T addLast) {
        synchronized (locker) {
            T head = null;
            while (size() >= maxSize) {
                head = poll();
            }
            addLast(addLast);
            return head;
        }
    }
 
    public T pollSafe() {
        synchronized (locker) {
            return poll();
        }
    }
  
    public int getMaxSize() {
        return this.maxSize;
    }
}
