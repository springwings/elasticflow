/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.node;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.yarn.coord.ReportStatus;
import org.elasticflow.yarn.monitor.ResourceMonitor;

/**
 * Safe exit system
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:23
 */
public class SafeShutDown extends Thread {
	@Override
	public void run() {
		if (GlobalParam.DISTRIBUTE_RUN) {
			if (EFNodeUtil.isMaster()) {
				Common.LOG.warn("cluster master is closed.");
				GlobalParam.INSTANCE_COORDER.distributeCoorder().stopAllNodes(true);
				Common.stopSystem(false);
			} else {
				// close heartBeat and try leave cluster
				if (ReportStatus.heartBeatIsOn()) {
					try {
						Common.LOG.info("start leave cluser...");
						ResourceMonitor.stop();
						GlobalParam.DISCOVERY_COORDER.leaveCluster(GlobalParam.IP, GlobalParam.NODEID);
						Common.LOG.info("leave cluser success.");
					} catch (Exception e) {
						Common.LOG.warn("leave cluster failed, master is offline.");
					}
					Common.stopSystem(false);
				}
			}
		} else {
			stopAllInstances();
		}
	}

	/**
	 * Task stop controlling local operation mode
	 */
	public static void stopAllInstances() {
		String[] instances = GlobalParam.StartConfig.getProperty("instances").split(",");
		for (int i = 0; i < instances.length; i++) {
			String[] strs = instances[i].split(":");
			if (strs.length <= 0 || strs[0].length() < 1)
				continue;
			if (Integer.parseInt(strs[1]) > 0) {
				GlobalParam.INSTANCE_COORDER.stopInstance(strs[0], GlobalParam.JOB_TYPE.FULL.name());
				GlobalParam.INSTANCE_COORDER.stopInstance(strs[0], GlobalParam.JOB_TYPE.FULL.name());
			}
		}
	}
}
