package org.elasticflow.flow;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public interface Socket<T> {
	
	T getSocket(Object... args);
	
}
