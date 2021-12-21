package org.elasticflow.yarn.coord;

/**
 * EFMonitor interface
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public interface EFMonitorCoord extends Coordination{
	
	public String getStatus(String poolName);
	
}
