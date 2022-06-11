/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.monitor;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.yarn.coord.ReportStatus;
import org.elasticflow.yarn.coordinator.node.DistributeService;

import com.alibaba.fastjson.JSON;

/**
 * Cluster resource monitoring
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-12-04 13:39
 */

public class ResourceMonitor {

	static DistributeService distributeService = new DistributeService();

	public static void start() throws EFException {
		ReportStatus.nodeConfigs();
		EFDataStorer.setData(GlobalParam.CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID + "/configs",
				JSON.toJSONString(GlobalParam.StartConfig));
		distributeService.start();
		if (EFNodeUtil.isSlave()) {
			ReportStatus.openHeartBeat();
		}
	}

	public static void stop() {
		if (EFNodeUtil.isSlave()) {
			ReportStatus.closeHeartBeat();
		} else {
			distributeService.stop();
		}
	}
}
