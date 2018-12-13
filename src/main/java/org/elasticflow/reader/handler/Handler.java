package org.elasticflow.reader.handler;

import java.util.HashMap;

/**
 * user defined read data process function
 * @author chengwen
 * @version 1.0 
 */
public interface Handler{
	public <T>T handlePage(Object... args);
	public <T>T handleData(Object... args);
	public boolean loopScan(HashMap<String, String> params);
}
