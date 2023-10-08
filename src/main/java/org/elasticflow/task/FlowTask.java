/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.task;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.MECHANISM;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.Localization;
import org.elasticflow.model.Localization.LAG_TYPE;
import org.elasticflow.node.CPU;
import org.elasticflow.piper.Breaker;
import org.elasticflow.piper.PipePump;
import org.elasticflow.piper.Valve;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.EFException.ETYPE;
import org.elasticflow.util.instance.EFTuple;
import org.elasticflow.util.instance.EFWriterUtil;
import org.elasticflow.yarn.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * schedule task description,to manage task job
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-27 10:23
 */
public class FlowTask {

	private boolean recompute = true;
	/** Used to control multiple tasks to write data to the same place */
	private boolean isReferenceInstance = false;
	private PipePump pipePump;
	public Breaker breaker;
	public Valve valve;
	/**
	 * seq for scan series datas
	 */
	private String L1seq = "";

	/** write real destination */
	private String destination;
	private String instanceID;

	private final static Logger log = LoggerFactory.getLogger(FlowTask.class);

	public static FlowTask createTask(PipePump pipePump) {
		return new FlowTask(pipePump, GlobalParam.DEFAULT_RESOURCE_SEQ);
	}

	public static FlowTask createTask(PipePump pipePump, String L1seq) {
		return new FlowTask(pipePump, L1seq);
	}

	private FlowTask(PipePump pipePump, String L1seq) {
		this.pipePump = pipePump;
		this.L1seq = L1seq;
		if (pipePump.getInstanceConfig().getPipeParams().getReferenceInstance() != null)
			isReferenceInstance = true;
		breaker = new Breaker();
		valve = new Valve();
		breaker.init(pipePump.getInstanceConfig().getInstanceID(),
				pipePump.getInstanceConfig().getPipeParams().getFailFreq(),
				pipePump.getInstanceConfig().getPipeParams().getMaxFailTime());
		destination = getDestination();
		instanceID = pipePump.getInstanceID();
	}

	/**
	 * if no full job will auto open optimize job
	 * 
	 * @throws EFException
	 */
	public void optimizeInstance() throws EFException {
		String storeName = Common.getInstanceRunId(pipePump.getInstanceID(), L1seq);
		CPU.RUN(pipePump.getID(), "Pond", "optimizeInstance", true, storeName,
				GlobalParam.TASK_COORDER.getStoreId(pipePump.getInstanceID(), L1seq, pipePump.getID(), true, false));
	}

	public void setRecompute(boolean recompute) {
		this.recompute = recompute;
	}

	/**
	 * slave instance full job
	 */
	public void runFull() {
		if (runConditionCheck() == false)
			return;
		if (GlobalParam.TASK_COORDER.setFlowStatus(instanceID, L1seq, GlobalParam.JOB_TYPE.FULL.name(), STATUS.Ready,
				STATUS.Running, pipePump.getInstanceConfig().getPipeParams().showInfoLog())) {
			try {
				String storeId = GlobalParam.TASK_COORDER.getStoreId(destination, L1seq, pipePump.getID(), false,
						false);
				if (!isReferenceInstance)
					CPU.RUN(pipePump.getID(), "Pond", "createStorePosition", true,
							Common.getInstanceRunId(destination, L1seq), storeId);
				if (storeId != null) {
					GlobalParam.TASK_COORDER.scanPositionkeepCurrentPos(instanceID);
					pipePump.run(storeId, L1seq, true, isReferenceInstance);
					GlobalParam.TASK_COORDER.scanPositionRecoverKeep(instanceID);
					GlobalParam.TASK_COORDER.saveTaskInfo(instanceID, L1seq, storeId, false);
					for (String L2seq : pipePump.getInstanceConfig().getReaderParams().getL2Seq()) {
						GlobalParam.TASK_COORDER.setScanPosition(instanceID, L1seq, L2seq, "", true, true);
					}
					GlobalParam.TASK_COORDER.saveTaskInfo(instanceID, L1seq, storeId, true);
				}
				runNextJobs(JOB_TYPE.FULL);
			} catch (Exception e) {
				breaker.log();
				log.error(instanceID + " Full Exception", e);
				Resource.EfNotifier.send(Localization.format(LAG_TYPE.fullFail, instanceID), instanceID, e.getMessage(),
						EFException.ETYPE.DATA_ERROR.name(), false);
			} finally {
				GlobalParam.TASK_COORDER.setFlowStatus(instanceID, L1seq, GlobalParam.JOB_TYPE.FULL.name(),
						STATUS.Blank, STATUS.Ready, pipePump.getInstanceConfig().getPipeParams().showInfoLog());
			}
		} else {
			if (pipePump.getInstanceConfig().getPipeParams().getLogLevel() == 0)
				log.info(instanceID + " still running，new full job is aborted!");
		}
	}

	/**
	 * Primary virtual node full task running
	 */
	public void runVirtualFull() {
		if (runConditionCheck() == false)
			return;
		if (GlobalParam.TASK_COORDER.setFlowStatus(instanceID, L1seq, GlobalParam.JOB_TYPE.FULL.name(), STATUS.Ready,
				STATUS.Running, pipePump.getInstanceConfig().getPipeParams().showInfoLog())) {
			try {
				String storeId = GlobalParam.TASK_COORDER.getStoreId(destination, L1seq, pipePump.getID(), false,
						false);
				CPU.RUN(pipePump.getID(), "Pond", "createStorePosition", true,
						Common.getInstanceRunId(destination, L1seq), storeId);
				GlobalParam.TASK_COORDER.setFlowInfo(instanceID, GlobalParam.JOB_TYPE.VIRTUAL.name(),
						GlobalParam.FLOWINFO.FULL_JOBS.name(),
						getNextJobs(pipePump.getInstanceConfig().getPipeParams().getNextJob()));
				runNextJobs(JOB_TYPE.FULL);
			} catch (Exception e) {
				log.error(instanceID + " Virtual Full Exception", e);
				Resource.EfNotifier.send(instanceID + " Virtual Full Exception", instanceID, e.getMessage(),
						EFException.ETYPE.DATA_ERROR.name(), false);
			} finally {
				GlobalParam.TASK_COORDER.setFlowStatus(instanceID, L1seq, GlobalParam.JOB_TYPE.FULL.name(),
						STATUS.Blank, STATUS.Ready, pipePump.getInstanceConfig().getPipeParams().showInfoLog());
			}
		} else {
			if (pipePump.getInstanceConfig().getPipeParams().getLogLevel() == 0)
				log.info(instanceID + " still running，new virtual full job is aborted!");
		}
	}

	/**
	 * Primary virtual node Increment task running
	 * 
	 * @throws EFException
	 */
	public void runVirtualIncrement() {
		if (runConditionCheck() == false)
			return;
		if (GlobalParam.TASK_COORDER.setFlowStatus(instanceID, L1seq, GlobalParam.JOB_TYPE.INCREMENT.name(),
				STATUS.Ready, STATUS.Running, pipePump.getInstanceConfig().getPipeParams().showInfoLog())) {
			try {
				String storeId = GlobalParam.TASK_COORDER.getStoreId(destination, L1seq, pipePump.getID(), true,
						recompute);
				GlobalParam.TASK_COORDER.setFlowInfo(instanceID, GlobalParam.JOB_TYPE.VIRTUAL.name(),
						GlobalParam.FLOWINFO.INCRE_STOREID.name(), storeId);
				runNextJobs(JOB_TYPE.INCREMENT);
			} catch (Exception e) {
				log.error(instanceID + " Virtual Increment Exception", e);
				Resource.EfNotifier.send(instanceID + " Virtual Increment Exception", instanceID, e.getMessage(),
						EFException.ETYPE.DATA_ERROR.name(), false);
			} finally {
				GlobalParam.TASK_COORDER.setFlowStatus(instanceID, L1seq, GlobalParam.JOB_TYPE.INCREMENT.name(),
						STATUS.Blank, STATUS.Ready, pipePump.getInstanceConfig().getPipeParams().showInfoLog());
				recompute = false;
			}
		} else {
			if (pipePump.getInstanceConfig().getPipeParams().getLogLevel() == 0)
				log.info(instanceID + " still running，new virtual increment job is aborted!");
		}
	}

	/**
	 * slave instance increment job
	 */
	public void runIncrement() {
		if (runConditionCheck() == false) 
			return;
		if (GlobalParam.TASK_COORDER.setFlowStatus(instanceID, L1seq, GlobalParam.JOB_TYPE.INCREMENT.name(),
				STATUS.Ready, STATUS.Running, pipePump.getInstanceConfig().getPipeParams().showInfoLog())) {
			String storeId = null;
			try {
				storeId = GlobalParam.TASK_COORDER.getStoreId(destination, L1seq, pipePump.getID(), true,
						(isReferenceInstance ? false : recompute));
				//Determine whether the storage ID can be obtained. 
				//If it cannot be obtained, interrupt the batch processing operation of the task.
				if(storeId == null) { 
					throw new EFException("get "+destination+" storage location exception!",ELEVEL.Dispose,ETYPE.RESOURCE_ERROR); 
				}else {
					GlobalParam.TASK_COORDER.saveTaskInfo(instanceID, L1seq, storeId, false);
					pipePump.run(storeId, L1seq, false, isReferenceInstance);
					runNextJobs(JOB_TYPE.INCREMENT);
				} 
			} catch (EFException e) {
				breaker.log();
				if (!isReferenceInstance && e.getErrorType() == ETYPE.RESOURCE_ERROR) {
					log.error("get {} storage location exception!", destination, e);
					breaker.openBreaker();
					Resource.EfNotifier.send(Localization.format(LAG_TYPE.FailPosition, destination), instanceID,
							e.getMessage(), e.getErrorType().name(), false);
				} else if (e.getErrorType() == ETYPE.EXTINTERRUPT) {
					log.warn("{}_{} increment external interrupt!",instanceID,L1seq);
				} else {
					log.error(instanceID + "_" +L1seq+ " increment job exception", e);
				} 
			} finally {
				recompute = this.checkReCompute(storeId);
				GlobalParam.TASK_COORDER.setFlowStatus(instanceID, L1seq, GlobalParam.JOB_TYPE.INCREMENT.name(),
						STATUS.Blank, STATUS.Ready, pipePump.getInstanceConfig().getPipeParams().showInfoLog());
			}
		} else {
			if (pipePump.getInstanceConfig().getPipeParams().getLogLevel() == 0)
				log.info(instanceID + " still running，new increment job is aborted!");
		}
	}

	/**
	 * It is mainly used to verify whether the new write position needs to be
	 * switched under the time write mechanism
	 * 
	 * @return
	 */
	private boolean checkReCompute(String storeId) {
		if (pipePump.getInstanceConfig().getPipeParams().getWriteMechanism() == MECHANISM.Time) {
			EFTuple<Long, Long> dTuple = EFWriterUtil.timeMechanism(pipePump.getInstanceConfig());
			if (storeId == null || !String.valueOf(dTuple.v1).equals(storeId)) {
				return true;
			}
		}
		return false;
	}

	private boolean runConditionCheck() {
		if (breaker.isOn()) {
			if(breaker.isFirstNotify) {
				pipePump.getReader().clearPool();
				pipePump.getWriter().clearPool();
			}
			return false;
		}			
		if (valve.isOn())
			return false;
		return true;
	}

	/**
	 * 
	 * @param isFull
	 * @return destination
	 */
	private String getDestination() {
		String destination;
		if (isReferenceInstance) {
			destination = pipePump.getInstanceConfig().getPipeParams().getReferenceInstance();
		} else {
			destination = pipePump.getInstanceID();
		}
		return destination;
	}

	private void runNextJobs(JOB_TYPE jobtype) {
		if (GlobalParam.DISTRIBUTE_RUN) {
			for (String slave : pipePump.getInstanceConfig().getPipeParams().getNextJob()) {
				GlobalParam.DISCOVERY_COORDER.runInstanceNow(slave, jobtype.name(),
						pipePump.getInstanceConfig().getPipeParams().isAsync());
			}
		} else {
			for (String slave : pipePump.getInstanceConfig().getPipeParams().getNextJob()) {
				Resource.flowCenter.runInstanceNow(slave, jobtype.name(),
						pipePump.getInstanceConfig().getPipeParams().isAsync());
			}
		}
	}

	private static String getNextJobs(String[] nextJobs) throws EFException {
		StringBuilder sf = new StringBuilder();
		for (String job : nextJobs) {
			InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(job);
			if (instanceConfig.openTrans()) {
				String[] _seqs = Common.getL1seqs(instanceConfig);
				for (String seq : _seqs) {
					if (seq == null)
						continue;
					sf.append(Common.getInstanceRunId(job, seq) + " ");
				}
			}
		}
		return sf.toString();
	}
}
