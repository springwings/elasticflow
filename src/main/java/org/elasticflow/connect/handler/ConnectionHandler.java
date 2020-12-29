package org.elasticflow.connect.handler;

import org.elasticflow.param.pipe.ConnectParams;

/**
 * User defined function for connection preprocessing.
 * @author chengwen
 * @version 1.0 
 */
public interface ConnectionHandler {
	void init(ConnectParams Params);
	public String getData();
}
