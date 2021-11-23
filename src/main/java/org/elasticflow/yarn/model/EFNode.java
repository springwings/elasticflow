package org.elasticflow.yarn.model;

import org.elasticflow.util.Common;

/**
* @description 
* @author chengwen
* @version 1.0
* @date 2018-11-13 10:53
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
