/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.task;

import java.util.HashMap;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.node.CPU;
import org.elasticflow.piper.Breaker;
import org.elasticflow.piper.PipePump;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ETYPE;
import org.elasticflow.yarn.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * schedule task description,to manage task job
 * @author chengwen
 * @version 2.0
 * @date 2018-11-27 10:23
 */
public class FlowTask {

	private boolean recompute = true;
	private boolean writeInSamePosition = false;
	private String instance;
	private PipePump transDataFlow;
	private Breaker breaker;
	/**
	 * seq for scan series datas
	 */
	private String L1seq = "";

	private final static Logger log = LoggerFactory.getLogger(FlowTask.class);

	public static FlowTask createTask(String instanceName, PipePump transDataFlow) {
		return new FlowTask(instanceName, transDataFlow, GlobalParam.DEFAULT_RESOURCE_SEQ);
	}

	public static FlowTask createTask(String instanceName, PipePump transDataFlow, String L1seq) {
		return new FlowTask(instanceName, transDataFlow, L1seq);
	}

	private FlowTask(String instance, PipePump transDataFlow, String L1seq) {
		this.instance = instance;
		this.transDataFlow = transDataFlow;
		this.L1seq = L1seq;
		if (transDataFlow.getInstanceConfig().getPipeParams().getInstanceName() != null)
			writeInSamePosition = true;//c write in same position
		breaker = new Breaker();
		breaker.init();
	}

	/**
	 * if no full job will auto open optimize job
	 */
	public void optimizeInstance() {
		String storeName = Common.getMainName(instance, L1seq);
		CPU.RUN(transDataFlow.getID(), "Pond", "optimizeInstance", true, storeName,
				Common.getStoreId(instance, L1seq, transDataFlow, true, false));
	}

	/**
	 * slave instance full job
	 */
	public void runFull() {
		if (!breaker.isOn() && Common.setFlowStatus(instance,L1seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Ready,STATUS.Running)) {
			try { 
				String storeId=null;
				if (writeInSamePosition && Resource.FLOW_INFOS.containsKey(transDataFlow.getInstanceConfig().getPipeParams().getInstanceName(),
						GlobalParam.FLOWINFO.MASTER.name())) {
					storeId = Resource.FLOW_INFOS
							.get(transDataFlow.getInstanceConfig().getPipeParams().getInstanceName(),
									GlobalParam.FLOWINFO.MASTER.name())
							.get(GlobalParam.FLOWINFO.FULL_STOREID.name()); 
				} else {
					storeId = Common.getStoreId(instance, L1seq, transDataFlow, false, false);
					CPU.RUN(transDataFlow.getID(), "Pond", "createStorePosition", true,
							Common.getMainName(instance, L1seq), storeId);
				}
				if(storeId!=null) {
					GlobalParam.SCAN_POSITION.get(Common.getMainName(instance, L1seq)).keepCurrentPos();
					transDataFlow.run(instance, storeId, L1seq, true,
							writeInSamePosition);
					GlobalParam.SCAN_POSITION.get(Common.getMainName(instance, L1seq)).recoverKeep(); 
					Common.saveTaskInfo(instance, L1seq, storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
				} 
			} catch (Exception e) {
				breaker.log();
				log.error(instance + " Full Exception", e);
			} finally {
				Common.setFlowStatus(instance,L1seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Blank,STATUS.Ready);
			}
		} else {
			if(transDataFlow.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(instance + " current start full computational task has been breaked!");
		}
	}

	public void runMasterFull() {
		if (Common.setFlowStatus(instance,L1seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Ready,STATUS.Running)) {
			try {
				String storeId = Common.getStoreId(instance, L1seq, transDataFlow, false, false); 
				String destination = instance;
				if(writeInSamePosition) 
					destination = transDataFlow.getInstanceConfig().getPipeParams().getInstanceName(); 
				
				if (!Resource.FLOW_INFOS.containsKey(destination, GlobalParam.FLOWINFO.MASTER.name())) 
					Resource.FLOW_INFOS.set(destination, GlobalParam.FLOWINFO.MASTER.name(),
						new HashMap<String, String>());
				Resource.FLOW_INFOS.get(destination, 
						GlobalParam.FLOWINFO.MASTER.name()).put(GlobalParam.FLOWINFO.FULL_STOREID.name(), storeId);

				CPU.RUN(transDataFlow.getID(), "Pond", "createStorePosition", true,
						Common.getMainName(instance, L1seq), storeId);

				Resource.FLOW_INFOS.get(destination, GlobalParam.FLOWINFO.MASTER.name()).put(
						GlobalParam.FLOWINFO.FULL_JOBS.name(),
						getNextJobs(transDataFlow.getInstanceConfig().getPipeParams().getNextJob()));
				
				for (String slave : transDataFlow.getInstanceConfig().getPipeParams().getNextJob()) { 
					Resource.FlOW_CENTER.runInstanceNow(slave, "full",transDataFlow.getInstanceConfig().getPipeParams().isAsync());
				}
			} catch (Exception e) {
				log.error(instance + " Full Exception", e);
			} finally {
				Common.setFlowStatus(instance,L1seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Blank,STATUS.Ready);
			}
		} else {
			if(transDataFlow.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(instance + " Current Master Full flow has been breaked!");
		}
	}

	public void runMasterIncrement() {
		if (Common.setFlowStatus(instance,L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Ready,STATUS.Running)) {
			try {
				String storeId = Common.getStoreId(instance, L1seq, transDataFlow, true, recompute); 
				String destination = instance;
				if(writeInSamePosition)
					destination = transDataFlow.getInstanceConfig().getPipeParams().getInstanceName();
				
				if (!Resource.FLOW_INFOS.containsKey(destination, GlobalParam.FLOWINFO.MASTER.name())) 
					Resource.FLOW_INFOS.set(destination, GlobalParam.FLOWINFO.MASTER.name(),
						new HashMap<String, String>());
				Resource.FLOW_INFOS.get(destination, 
						GlobalParam.FLOWINFO.MASTER.name()).put(GlobalParam.FLOWINFO.INCRE_STOREID.name(), storeId);
				
				for (String slave : transDataFlow.getInstanceConfig().getPipeParams().getNextJob()) {
					Resource.FlOW_CENTER.runInstanceNow(slave, "increment",transDataFlow.getInstanceConfig().getPipeParams().isAsync());
				}
			} finally {
				Common.setFlowStatus(instance,L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Blank,STATUS.Ready);  
				recompute = false;
			}
		} else {
			if(transDataFlow.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(instance + " Current Master Increment flow has been breaked!");
		}
	}

	/**
	 * slave instance increment job
	 */
	public void runIncrement() {  
		if (!breaker.isOn() && Common.setFlowStatus(instance,L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Ready,STATUS.Running)) {
			String storeId;
			if (writeInSamePosition) {
				storeId = Resource.FLOW_INFOS.get(transDataFlow.getInstanceConfig().getPipeParams().getInstanceName(),
						GlobalParam.FLOWINFO.MASTER.name()).get(GlobalParam.FLOWINFO.INCRE_STOREID.name());
				Common.setAndGetScanInfo(instance, L1seq, storeId);
			} else {
				storeId = Common.getStoreId(instance, L1seq, transDataFlow, true, recompute);
			}
 
			try {
				transDataFlow.run(instance, storeId, L1seq, false, writeInSamePosition); 
			} catch (EFException e) {
				if (!writeInSamePosition && e.getErrorType()==ETYPE.WRITE_POS_NOT_FOUND) {
					storeId = Common.getStoreId(instance, L1seq, transDataFlow, true, true);
					try {
						transDataFlow.run(instance, storeId,L1seq, false, writeInSamePosition);
					} catch (EFException ex) {
						log.error(instance + " Increment Exception", ex);
					}
				}else {
					breaker.log();
				}
				log.error(instance + " IncrementJob Exception", e);
			} finally {
				recompute = false;
				Common.setFlowStatus(instance,L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Blank,STATUS.Ready); 
			}
		} else {
			if(transDataFlow.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(instance + " Current Increment flow has been breaked!");
		}
	}

	private static String getNextJobs(String[] nextJobs) {
		StringBuilder sf = new StringBuilder();
		for (String job : nextJobs) {
			InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(job);
			if (instanceConfig.openTrans()) {
				String[] _seqs = Common.getL1seqs(instanceConfig, true);
				for (String seq : _seqs) {
					if (seq == null)
						continue;
					sf.append(Common.getMainName(job, seq) + " ");
				}
			}
		}
		return sf.toString();
	} 
}
