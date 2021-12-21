package org.elasticflow.yarn.coord;

import com.alibaba.fastjson.JSONObject;

/**
 * EFMonitor interface
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public interface EFMonitorCoord extends Coordination{
	
	public String getStatus(String poolName);
	
	public JSONObject getPipeEndStatus(String instance, String L1seq);
	
}
