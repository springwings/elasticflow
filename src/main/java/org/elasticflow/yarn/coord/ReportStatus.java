/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coord;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.yarn.Resource;

/**
 * Report EF machine node status.
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-21 15:43
 */
public final class ReportStatus {
	
	static boolean openHeartBeat = true;
	
	public static void closeHeartBeat() {
		openHeartBeat = false;
	}

	public static void openHeartBeat() {
		if (EFNodeUtil.isMaster() == false) {
			openHeartBeat = true;
			Resource.ThreadPools.execute(() -> {
				while (openHeartBeat) {
					try {
						Thread.sleep(GlobalParam.NODE_LIVE_TIME/2);
						GlobalParam.DISCOVERY_COORDER.reportStatus();						
					} catch (Exception e) {
						Common.LOG.warn("master node cannot connect.");
					}
				}
			});
		}
	}

	public static void nodeConfigs() {
		try {
			if (EFDataStorer.exists(GlobalParam.CONFIG_PATH) == false) {
				String path = "";
				for (String str : GlobalParam.CONFIG_PATH.split("/")) {
					path += "/" + str;
					EFDataStorer.createPath(path, false);
				}
			}
			if (EFDataStorer.exists(GlobalParam.CONFIG_PATH + "/INSTANCES") == false)
				EFDataStorer.createPath(GlobalParam.CONFIG_PATH + "/INSTANCES", false);
			if (EFDataStorer.exists(GlobalParam.CONFIG_PATH + "/instructions.xml") == false)
				EFDataStorer.createPath(GlobalParam.CONFIG_PATH + "/instructions.xml", true);
			if (EFDataStorer.exists(GlobalParam.CONFIG_PATH + "/resource.xml") == false)
				EFDataStorer.createPath(GlobalParam.CONFIG_PATH + "/resource.xml", true);

			if (EFDataStorer.exists(GlobalParam.CONFIG_PATH + "/EF_NODES") == false) {
				EFDataStorer.createPath(GlobalParam.CONFIG_PATH + "/EF_NODES", false);
			}
			if (EFDataStorer.exists(GlobalParam.CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID) == false) {
				EFDataStorer.createPath(GlobalParam.CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID, false);
			}
			if (EFDataStorer
					.exists(GlobalParam.CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID + "/configs") == false) {
				EFDataStorer.createPath(GlobalParam.CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID + "/configs", true);
			}
		} catch (Exception e) {
			Common.LOG.error("environmentCheck Exception", e);
		}
	}

}
