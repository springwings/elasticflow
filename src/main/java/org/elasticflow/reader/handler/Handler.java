/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
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
