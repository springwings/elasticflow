/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.piper;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticflow.config.GlobalParam.ETYPE;
import org.elasticflow.model.FIFOQueue;
import org.elasticflow.model.Localization;
import org.elasticflow.model.Localization.LAG_TYPE;
import org.elasticflow.util.Common;
import org.elasticflow.yarn.Resource;

/**
 * pipe end connect breaker control When a serious error occurs, temporarily
 * interrupt the operation
 * 
 * @author chengwen
 * @version 1.0
 * @date 2019-01-24 10:55
 * @modify 2019-01-24 10:55
 */
@NotThreadSafe
public class Breaker {

	private long earlyFailTime;

	private boolean openBreaker = false;

	public boolean isFirstNotify = true;

	private int failTimes;

	// per-fail use time in last period
	private int perFailTime = 200;

	// Maximum fail times
	private int maxFailTime = 1000;

	private FIFOQueue<Long> queue = new FIFOQueue<>(6);

	private String instanceID; 

	public void init(String instanceID, int failFreq, int maxFailTime) {
		this.instanceID = instanceID;
		this.perFailTime = 1000 / failFreq;
		this.maxFailTime = maxFailTime;
		this.reset();
	}

	public void reset() {
		this.failTimes = 0;
		this.earlyFailTime = 0;
		this.isFirstNotify = true; 
		this.queue.clear();
		this.closeBreaker();
	}

	public void log() {
		if (this.earlyFailTime == 0)
			this.earlyFailTime = System.currentTimeMillis();
		queue.addLastSafe(System.currentTimeMillis());
		this.failTimes++;
	}

	public long failInterval() {
		if (queue.size() > 3) {
			return queue.getLast() - queue.getFirst();
		} else {
			return Integer.MAX_VALUE;
		}
	}

	public int getFailTimes() {
		return this.failTimes;
	}

	public String getReason() {
		StringBuffer sb = new StringBuffer();
		sb.append("breaker is manual opening:" + String.valueOf(this.openBreaker));
		sb.append(",fail times:" + String.valueOf(this.failTimes));
		sb.append(",fail interval:" + String.valueOf(failInterval()));
		sb.append(",fail times > " + String.valueOf(maxFailTime) + " OR fail Interval < " + String.valueOf(perFailTime));
		sb.append(",opening time of breaker "+Common.FormatTime(this.queue.getLast()));
		return sb.toString();
	}

	public void openBreaker() {
		this.openBreaker = true;
	}

	public void closeBreaker() {
		this.openBreaker = false;
	}

	public boolean isOn() {
		if (this.openBreaker || this.failTimes >= maxFailTime || failInterval() <= perFailTime) {
			if (isFirstNotify) { 
				Common.LOG.warn(Localization.formatEN(LAG_TYPE.flowBreaker, instanceID));
				Resource.EfNotifier.send(Localization.format(LAG_TYPE.flowBreaker, instanceID), instanceID,
						Localization.format(LAG_TYPE.flowDisconnect), ETYPE.RESOURCE_ERROR.name(), false);
			}
			isFirstNotify = false;
			return true;
		}
		return false;
	}
}
