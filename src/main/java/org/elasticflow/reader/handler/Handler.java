package org.elasticflow.reader.handler;

import org.elasticflow.model.Task;


/**
 * user defined read data process function
 * @author chengwen
 * @version 2.0
 * @date 2018-12-28 09:27
 */
public abstract class Handler{
	
	public abstract <T>T handlePage(Object... args);
	
	public abstract <T>T handleData(Object... args);
	
	public boolean loopScan(Task task) {
		return false;
	}
}
