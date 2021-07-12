/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.model.reader;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class ReaderState { 
	private String ReaderScanStamp = "0";
	private int count = 0;
	boolean status = true;
	
 
	public String getReaderScanStamp() {
		return ReaderScanStamp;
	}
	public void setReaderScanStamp(String ReaderScanStamp) {
		this.ReaderScanStamp = ReaderScanStamp;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public boolean isStatus() {
		return status;
	}
	public void setStatus(boolean status) {
		this.status = status;
	} 
}
