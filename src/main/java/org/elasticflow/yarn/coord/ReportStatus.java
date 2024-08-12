/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coord;

import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
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
	
	private static boolean openHeartBeat = true;
	
	private static boolean successConnect = false;
	
	public static void closeHeartBeat() {
		openHeartBeat = false;
	}
	
	public static boolean heartBeatIsOn() {
		return openHeartBeat;
	}

	public static void openHeartBeat() {
		if (EFNodeUtil.isMaster() == false) {
			openHeartBeat = true;
			Resource.threadPools.execute(() -> {
				while (openHeartBeat) {
					try {
						Thread.sleep(GlobalParam.NODE_LIVE_TIME/2);
						if(openHeartBeat) {
							GlobalParam.DISCOVERY_COORDER.reportStatus(GlobalParam.IP, GlobalParam.NODEID);	
							if(successConnect==false)
								Common.LOG.info("master node successfully connected！");
							successConnect = true;
						} 				
					} catch (Exception e) {
						successConnect = false;
						Common.LOG.warn("master node connect failed！");
						Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
						if(configMap.size()>0) {
							closeHeartBeat();
							Common.stopSystem(false);
						}
					}
				}
			});
		}
	}

	public static void nodeConfigs() {
		try {
			if (EFDataStorer.exists(GlobalParam.DATAS_CONFIG_PATH) == false) {
				String path = "";
				for (String str : GlobalParam.DATAS_CONFIG_PATH.split("/")) {
					path += "/" + str;
					EFDataStorer.createPath(path, false);
				}
			}
			if (EFDataStorer.exists(GlobalParam.DATAS_CONFIG_PATH + "/INSTANCES") == false)
				EFDataStorer.createPath(GlobalParam.DATAS_CONFIG_PATH + "/INSTANCES", false);
			if (EFDataStorer.exists(GlobalParam.DATAS_CONFIG_PATH + "/instructions.xml") == false)
				EFDataStorer.createPath(GlobalParam.DATAS_CONFIG_PATH + "/instructions.xml", true);
			if (EFDataStorer.exists(GlobalParam.DATAS_CONFIG_PATH + "/resource.xml") == false)
				EFDataStorer.createPath(GlobalParam.DATAS_CONFIG_PATH + "/resource.xml", true);

			if (EFDataStorer.exists(GlobalParam.DATAS_CONFIG_PATH + "/EF_NODES") == false) {
				EFDataStorer.createPath(GlobalParam.DATAS_CONFIG_PATH + "/EF_NODES", false);
			}
			if (EFDataStorer.exists(GlobalParam.DATAS_CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID) == false) {
				EFDataStorer.createPath(GlobalParam.DATAS_CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID, false);
			}
			if (EFDataStorer
					.exists(GlobalParam.DATAS_CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID + "/status") == false) {
				EFDataStorer.createPath(GlobalParam.DATAS_CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID + "/status", true);
			}
			if (EFDataStorer
					.exists(GlobalParam.DATAS_CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID + "/configs") == false) {
				EFDataStorer.createPath(GlobalParam.DATAS_CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID + "/configs", true);
			}
		} catch (Exception e) {
			Common.LOG.error("prepare run space {} exception",GlobalParam.DATAS_CONFIG_PATH, e);
		}
	}

}
