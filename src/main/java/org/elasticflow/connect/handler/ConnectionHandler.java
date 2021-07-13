/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.connect.handler;

import org.elasticflow.param.pipe.ConnectParams;

/**
 * User defined function for connection preprocessing.
 * 
 * @author chengwen
 * @version 1.0
 */
public interface ConnectionHandler {
	
	void init(ConnectParams Params);

	public String getData();
}
