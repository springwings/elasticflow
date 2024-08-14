/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coordinator;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.yarn.coord.slave.DiscoveryCoord;

/**
 * Master slave node discovery and reporting coordinator
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public class DiscoveryCoordinator implements DiscoveryCoord {
	
	/**
	 * Report your own status to the main node
	 */
	@Override
	public void reportStatus(String ip, int nodeId) {
		GlobalParam.INSTANCE_COORDER.distributeCoorder().updateCluster(ip, nodeId);
	}
	
	/**
	 * Notify the master node to leave the cluster
	 */
	@Override
	public void leaveCluster(String ip, int nodeId) {
		GlobalParam.INSTANCE_COORDER.distributeCoorder().removeNode(ip, nodeId, true);
	}
	
	/**
	 * Record critical logs to the primary node
	 */
	@Override
	public void systemLog(String message, Object... args) {
		GlobalParam.INSTANCE_COORDER.distributeCoorder().systemLog(message, args);
	}

	@Override
	public boolean runInstanceNow(String instance, String type, boolean asyn) {
		return GlobalParam.INSTANCE_COORDER.distributeCoorder().runClusterInstanceNow(instance, type, asyn);
	}
}
