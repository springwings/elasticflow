package org.elasticflow.yarn.coordinator;

import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.yarn.coord.EFMonitorCoord;

/**
 * EFMonitor Coordinator
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */

public class EFMonitorCoordinator implements EFMonitorCoord{
	 
	public String getStatus(String poolName) {
		return EFConnectionPool.getStatus(poolName);
	}
 

}
