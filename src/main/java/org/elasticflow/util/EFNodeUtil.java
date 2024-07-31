/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.util;

import java.net.InetSocketAddress;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.config.GlobalParam.NODE_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.reader.ScanPosition;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.yarn.EFRPCService;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.DiscoveryCoord;
import org.elasticflow.yarn.coord.TaskStateCoord;

/**
 * system running control
 * @author chengwen
 * @version 1.0
 * @date 2019-01-15 11:07
 * @modify 2019-01-15 11:07
 */

public final class EFNodeUtil {

	/**
	 * init node start parameters
	 * 
	 * @param instanceConfig
	 * @throws EFException 
	 */
	public static void loadInstanceDatas(InstanceConfig instanceConfig) throws EFException { 
		String instance = instanceConfig.getInstanceID();
		ScanPosition sp = new ScanPosition(instance, "");
		try { 
			String[] L1seqs = TaskUtil.getL1seqs(instanceConfig);
			GlobalParam.TASK_COORDER.initFlowProgressInfo(instance);
			for (String L1seq : L1seqs) {
				GlobalParam.TASK_COORDER.initFlowSingal(instance, L1seq);					
			}
			sp.loadInfos(TaskUtil.getStoreTaskInfo(instance,false),false);
			sp.loadInfos(TaskUtil.getStoreTaskInfo(instance,true),true);
		} catch (Exception e) { 
			throw new EFException(e,"instance "+instance+" init exception.",ELEVEL.Stop);
		}
		GlobalParam.TASK_COORDER.initTaskDatas(instance,sp);
	}

	public static void runShell(String[] commands) {
		Process pc = null;
		try { 
			pc = Runtime.getRuntime().exec(commands);
			pc.waitFor();
		} catch (InterruptedException e) {
			Common.LOG.warn("system progress is killed!");
		} catch (Exception e) {
			Common.LOG.error("system try to run {} Exception",commands.toString(), e);
		} finally {
			if (pc != null) {
				pc.destroy();
			}
		}
	} 
	
	/**
	 * Non-distributed default is master mode startup
	 * @return
	 */
	public static boolean isMaster() {
		if(GlobalParam.DISTRIBUTE_RUN==false ||(GlobalParam.DISTRIBUTE_RUN==true && GlobalParam.node_type==NODE_TYPE.master)) {
			return true;
		}
		return false;
	}
	
	public static boolean isSlave() {
		if(GlobalParam.DISTRIBUTE_RUN==true && GlobalParam.node_type==NODE_TYPE.slave) {
			return true;
		}
		return false;
	}
	
	/**
	 * init slave Coorder
	 * Construct a coordinator that reports from the node to the master node
	 */
	public static void initSlaveCoorder() {
		Resource.threadPools.execute(() -> {
			boolean redo = true;
			while (redo) {
				try {
					GlobalParam.TASK_COORDER = EFRPCService.getRemoteProxyObj(TaskStateCoord.class, 
							new InetSocketAddress(GlobalParam.SystemConfig.getProperty("master_host"), GlobalParam.MASTER_SYN_PORT));			
					GlobalParam.DISCOVERY_COORDER = EFRPCService.getRemoteProxyObj(DiscoveryCoord.class, 
							new InetSocketAddress(GlobalParam.SystemConfig.getProperty("master_host"), GlobalParam.MASTER_SYN_PORT));
					redo = false;
				} catch (Exception e) { 
					GlobalParam.TASK_COORDER = null;
					GlobalParam.DISCOVERY_COORDER = null;
					Common.LOG.error("init Slave Coorder exception",e);
				}
			}
		});
	} 
}
