package org.elasticflow.reader.message; 

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:24
 */
public interface MessageHandler {
	public boolean handle(Object... args);
}
