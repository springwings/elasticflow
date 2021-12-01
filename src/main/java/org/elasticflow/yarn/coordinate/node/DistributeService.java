/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coordinate.node;

import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.DiscoveryCoord;
import org.elasticflow.yarn.coord.InstanceCoord;
import org.elasticflow.yarn.coord.TaskStateCoord;
import org.elasticflow.yarn.coorder.DiscoveryCoorder;
import org.elasticflow.yarn.coorder.InstanceCoorder;
import org.elasticflow.yarn.coorder.TaskStateCoorder;

/**
 * Rebalance Tasks over all nodes.
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-21 15:43
 */
public class DistributeService {

	public void start() {
		if (GlobalParam.DISTRIBUTE_RUN == false) {
			if ((GlobalParam.SERVICE_LEVEL & 2) > 0) {
				Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
				for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) {
					Resource.FlOW_CENTER.addFlowGovern(entry.getKey(), entry.getValue(), false);
				}
			}
		} else {
			instanceCoordStart();
		}
	}

	public void instanceCoordStart() {
		new Thread(new Runnable() {
			public void run() {
				try {
					DataReceiver dataReceiver;
					if(EFNodeUtil.isMaster()) {
						dataReceiver = new DataReceiver(GlobalParam.NODE_DATA_SYN_PORT);
						dataReceiver.register(TaskStateCoord.class, TaskStateCoorder.class);
						dataReceiver.register(DiscoveryCoord.class, DiscoveryCoorder.class);
					}else {
						dataReceiver = new DataReceiver(GlobalParam.NODE_INSTANCE_SYN_PORT);
						dataReceiver.register(InstanceCoord.class, InstanceCoorder.class);						
					}
					dataReceiver.start();
				} catch (Exception e) {
					Common.LOG.error("Instance Coord Exception", e);
				}
			}
		}).start();
	}
}
