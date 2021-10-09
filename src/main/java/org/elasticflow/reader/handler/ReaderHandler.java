/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.reader.handler;

import org.elasticflow.model.Page;
import org.elasticflow.model.Task;
import org.elasticflow.util.EFException;


/**
 * user defined read data process function
 * @author chengwen
 * @version 2.0
 * @date 2018-12-28 09:27
 */
public abstract class ReaderHandler{
	
	protected boolean supportHandlePage = false;
	
	protected boolean supportHandleData = false;
	
	public boolean supportHandlePage() {
		return supportHandlePage;
	}

	public boolean supportHandleData() {
		return supportHandleData;
	}
	
	public abstract <T>T handlePage(Object invokeObject,final Task task,int pageSize) throws EFException;
	
	public abstract void handleData(Object invokeObject,Object datas,Page page,int pageSize) throws EFException;
	
	public boolean loopScan(Task task) {
		return false;
	}
}
