/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.writer.handler;

import org.elasticflow.flow.FlowHandler;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.util.EFException;

import com.alibaba.fastjson.JSONObject;

/**
 * Write Handler interface
 * @author chengwen
 * @version 4.0
 * @date 2018-11-14 16:54
 * 
 */
public abstract class WriterHandler extends FlowHandler{	
	
	public JSONObject handlerDSL = null;
	
	public void init(JSONObject handlerDSL) {
		this.handlerDSL = handlerDSL;
	} 
	
	public abstract DataPage handleData(Context context,DataPage dataPage) throws EFException;
	
}
