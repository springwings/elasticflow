/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coord.slave;

import org.elasticflow.yarn.coord.master.Coordination;

/**
 * Cluster node discovery interface
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public interface DiscoveryCoord extends Coordination{
	
	public void reportStatus(String ip,int nodeId);
	
	public void systemLog(String message, Object... args);
	
	public void leaveCluster(String ip,int nodeId);
	
	/**Notify the cluster to start the task*/
	public boolean runInstanceNow(String instance,String type,boolean asyn);
	
}
