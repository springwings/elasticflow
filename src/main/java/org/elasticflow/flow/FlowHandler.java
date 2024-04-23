/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.flow;

import com.alibaba.fastjson.JSONObject;

/**
 * user defined flow process function
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-12-28 09:27
 */

public abstract class FlowHandler {
	
	public JSONObject handlerDSL = null;
	
	public abstract void release();
	
	public void init(JSONObject handlerDSL) {
		this.handlerDSL = handlerDSL;
	} 
	
}
