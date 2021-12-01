package org.elasticflow.yarn.model;

import org.elasticflow.util.Common;

/**
 * 
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public class EFNode {
	
	private String ip;
	
	private boolean isOnline = true;
	
	private double lastCheckTime = Common.getNow();

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public boolean isOnline() {
		return isOnline;
	}

	public void setOnline(boolean isOnline) {
		this.isOnline = isOnline;
	}

	public double getLastCheckTime() {
		return lastCheckTime;
	}

	public void setLastCheckTime(double lastCheckTime) {
		this.lastCheckTime = lastCheckTime;
	}
	
	
}
