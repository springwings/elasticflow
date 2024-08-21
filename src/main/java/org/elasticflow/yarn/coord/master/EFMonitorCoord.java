package org.elasticflow.yarn.coord.master;

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
	
	public String analyzeInstance(String instance);
		
	public void resetPipeEndStatus(String instance, String L1seq);
	/**warehouse resource status */
	public HashMap<String, JSONObject> getResourceStates();
	
	public String getLogs(int lines);
	
	public boolean clearLogs(boolean errorLogFile);
	
	public void resetErrorStates();
	
}
