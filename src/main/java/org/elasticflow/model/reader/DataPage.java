/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.model.reader;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.elasticflow.config.GlobalParam;

/**
 * Data page model
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class DataPage extends HashMap<String, Object> implements Cloneable{

	private static final long serialVersionUID = 8764060758588207664L;
	
	public String getScanStamp() {
		if(this.containsKey(GlobalParam.READER_LAST_STAMP))
			return String.valueOf(this.get(GlobalParam.READER_LAST_STAMP));
		return "";
	}
	
	public DataPage() {
		this.put(GlobalParam.READER_STATUS,true);
	}
	
	public void putData(ConcurrentLinkedQueue<PipeDataUnit> data) {
		this.put("__DATAS", data);
	}
	
	public void putDataBoundary(String data) {
		this.put("__MAX_ID", data);
	}
	
	public String getDataBoundary() {
		return String.valueOf(this.get("__MAX_ID"));
	}
	
	@SuppressWarnings("unchecked")
	public ConcurrentLinkedQueue<PipeDataUnit> getData() { 
		return (ConcurrentLinkedQueue<PipeDataUnit>) this.get("__DATAS");
	}  
	
	@Override
	public void clear() {
		super.clear();
		this.put(GlobalParam.READER_STATUS,true);
	}
	
	@Override
	public DataPage clone() { 
		DataPage dp  = (DataPage) super.clone();
		ConcurrentLinkedQueue<PipeDataUnit> dt = new ConcurrentLinkedQueue<>();
		dt.addAll(this.getData());
		dp.put("__DATAS", dt);
		return dp;
	}
}
