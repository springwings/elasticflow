/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coordinator.node;

import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.DiscoveryCoord;
import org.elasticflow.yarn.coord.EFMonitorCoord;
import org.elasticflow.yarn.coord.InstanceCoord;
import org.elasticflow.yarn.coord.NodeCoord;
import org.elasticflow.yarn.coord.TaskStateCoord;
import org.elasticflow.yarn.coordinator.DiscoveryCoordinator;
import org.elasticflow.yarn.coordinator.EFMonitorCoordinator;
import org.elasticflow.yarn.coordinator.InstanceCoordinator;
import org.elasticflow.yarn.coordinator.NodeCoordinator;
import org.elasticflow.yarn.coordinator.TaskStateCoordinator;

/**
 * Rebalance Tasks over all nodes.
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-21 15:43
 */
public class DistributeService {

	DataReceiver dataReceiver;

	private static boolean openMonitor = true;

	public static void closeMonitor() {
		openMonitor = false;
	}

	public static void openMonitor() {
		openMonitor = true;
	}

	public static boolean isOpenMonitor() {
		return openMonitor;
	}

	public void start() {
		if (EFNodeUtil.isMaster()) {
			boolean createSchedule = !GlobalParam.DISTRIBUTE_RUN;
			if ((GlobalParam.SERVICE_LEVEL & 2) > 0) {
				Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
				for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) {
					Resource.flowCenter.addFlowGovern(entry.getKey(), entry.getValue(), false, createSchedule);
				}
			}
		}
		if (GlobalParam.DISTRIBUTE_RUN)
			instanceCoordStart();

	}

	public void stop() {
		if (GlobalParam.DISTRIBUTE_RUN)
			dataReceiver.stop();
	}

	private void monitorNodes() {
		Resource.threadPools.execute(() -> {
			while (openMonitor) {
				try {
					Thread.sleep(GlobalParam.NODE_LIVE_TIME * 2);
					GlobalParam.INSTANCE_COORDER.distributeCoorder().clusterScan(true);
				} catch (Exception e) {
					Common.LOG.warn("monitor exception", e);
				}
			}
		});
	}

	private void instanceCoordStart() {
		Resource.threadPools.execute(() -> {
			try {
				if (EFNodeUtil.isMaster()) {
					// Channels that the slave node can operate on taskstatecoord and discoverycoord
					dataReceiver = new DataReceiver(GlobalParam.MASTER_SYN_PORT);
					dataReceiver.register(TaskStateCoord.class, TaskStateCoordinator.class);
					dataReceiver.register(DiscoveryCoord.class, DiscoveryCoordinator.class);
					monitorNodes();
				} else {
					// Channels that the master node can operate on InstanceCoord NodeCoord and
					// EFMonitorCoord
					// The master operates the channels on nodes in cluster mode
					dataReceiver = new DataReceiver(GlobalParam.SLAVE_SYN_PORT);
					dataReceiver.register(InstanceCoord.class, InstanceCoordinator.class);
					dataReceiver.register(NodeCoord.class, NodeCoordinator.class);
					dataReceiver.register(EFMonitorCoord.class, EFMonitorCoordinator.class);
				}
				dataReceiver.start();
			} catch (Exception e) {
				Common.LOG.error("Instance Coord Exception", e);
				Common.stopSystem(false);
			}
		});
	}
}
