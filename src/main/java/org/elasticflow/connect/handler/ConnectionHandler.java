package org.elasticflow.connect.handler;

import java.util.HashMap;

/**
 * user defined connection pre-process function
 * @author chengwen
 * @version 1.0 
 */
public interface ConnectionHandler {
	void init(HashMap<String, Object> Params);
	public String getData();
}
