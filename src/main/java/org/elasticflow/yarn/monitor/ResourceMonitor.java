/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.monitor;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.correspond.ReportStatus;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.yarn.coordinate.node.DistributeService;

import com.alibaba.fastjson.JSON;

/**
 * Cluster resource monitoring
 * @author chengwen
 * @version 1.0
 * @date 2018-12-04 13:39
 */

public class ResourceMonitor {   
	public static void start() { 
		ReportStatus.nodeConfigs();
		EFDataStorer.setData(GlobalParam.CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID + "/configs", JSON.toJSONString(GlobalParam.StartConfig)); 
		new DistributeService().start();
		if(!EFNodeUtil.isMaster()) {
			ReportStatus.heartBeat();
		}
	} 
}
