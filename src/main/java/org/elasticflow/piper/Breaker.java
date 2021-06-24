/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.piper;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * pipe end connect breaker control
 * When a serious error occurs, temporarily interrupt the operation
 * @author chengwen
 * @version 1.0
 * @date 2019-01-24 10:55
 * @modify 2019-01-24 10:55
 */
@NotThreadSafe
public class Breaker {

	private volatile long earlyFail;

	private volatile long checkFail;

	private int failTimes;
	
	private int failSpan = 300;
	
	private int recheckSpan = 60000;
	
	public void init() {
		this.failTimes = 0;
		this.earlyFail = 0;
		this.checkFail = 0;
	}

	public void log() {
		if (this.earlyFail == 0)
			this.earlyFail = System.currentTimeMillis();
		this.failTimes++;
	}

	public boolean isOn() {
		long current = System.currentTimeMillis();
		if (this.failTimes > 2 && current - earlyFail > failSpan ) {
			this.failTimes = 0;
			this.earlyFail = 0;
			this.checkFail = current;
			return true;
		}
		
		if (current - checkFail < recheckSpan)
			return true;
		
		return false;
	}
}
