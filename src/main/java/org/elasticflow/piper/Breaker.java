package org.elasticflow.piper;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * pipe end connect breaker control
 * 
 * @author chengwen
 * @version 1.0
 * @date 2019-01-24 10:55
 * @modify 2019-01-24 10:55
 */
@NotThreadSafe
public class Breaker {

	private long earlyFail;

	private long checkFail;

	private int failTimes;
	
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
		if (this.failTimes > 2 && current - earlyFail > 3000 ) {
			this.failTimes = 0;
			this.earlyFail = 0;
			this.checkFail = current;
			return true;
		}
		
		if (current - checkFail < 60000)
			return true;
		
		return false;
	}
}
