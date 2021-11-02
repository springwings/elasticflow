/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.param.warehouse;

import org.elasticflow.config.GlobalParam.DATA_SOURCE_TYPE;

import com.alibaba.fastjson.JSONObject;

/**
 * seq for series data position define
 * @author chengwen
 * @version 1.0 
 * @date 2018-07-22 09:08
 */
public interface WarehouseParam {
	
	public String[] getL1seq();
	
	public void setL1seq(String seqs);
	
	public DATA_SOURCE_TYPE getType();
	
	public String getHandler(); 
	
	public String getPoolName(String L1seq);
	
	public int getMaxConn();
	
	public void setMaxConn(String maxConn);
	
	public JSONObject getCustomParams();
	
	public JSONObject getDefaultValue();
}