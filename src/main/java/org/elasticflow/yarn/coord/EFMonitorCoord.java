package org.elasticflow.yarn.coord;

import java.util.HashMap;

import com.alibaba.fastjson.JSONObject;

/**
 * EFMonitor interface
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public interface EFMonitorCoord extends Coordination{
	
	public String getPoolStatus(String poolName);
	
	public HashMap<String, Object> getNodeStatus();
	
	public JSONObject getPipeEndStatus(String instance, String L1seq);
	
	public void resetPipeEndStatus(String instance, String L1seq);
	
}
