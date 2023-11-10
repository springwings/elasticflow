/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.model;

import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;

/**
 * ElasticFlow response model
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-05 13:53
 */
public class EFHttpResponse { 
	
	private RESPONSE_STATUS status; 
	private String payload = "";
	private String info = "";
	private long usems = 0;
	
	
	public static EFHttpResponse getInstance() {
		EFHttpResponse rs = new EFHttpResponse(); 
		rs.status = RESPONSE_STATUS.Success; 
		return rs;
	}
	
	public void setInfo(String info) {
		this.info = info; 
	}
	
	public void setUsems(long usems) {
		this.usems = usems;
	}

	public void setStatus(RESPONSE_STATUS status) { 
		this.status = status;
	} 

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}
	
	public RESPONSE_STATUS getStatus() {
		return this.status;
	}
	
	public boolean isSuccess() {
		if(this.status==RESPONSE_STATUS.Success)
			return true;
		return false;
	}
	
	public String getInfo() {
		return this.info;
	}
	
	public long getUsems() {
		return this.usems;
	}
}
