/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.task;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.MECHANISM;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.node.CPU;
import org.elasticflow.piper.Breaker;
import org.elasticflow.piper.PipePump;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ETYPE;
import org.elasticflow.util.instance.EFTuple;
import org.elasticflow.util.instance.EFWriterUtil;
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
	/**Used to control multiple tasks to write data to the same place*/
	private boolean writeInSamePosition = false;
	private PipePump pipePump;
	private Breaker breaker;
	/**
	 * seq for scan series datas
	 */
	private String L1seq = "";
 
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
		if (pipePump.getInstanceConfig().getPipeParams().getInstanceName() != null)
			writeInSamePosition = true;
		breaker = new Breaker();
		breaker.init(pipePump.getInstanceConfig().getInstanceID(),
				pipePump.getInstanceConfig().getPipeParams().getFailFreq(),
				pipePump.getInstanceConfig().getPipeParams().getMaxFailTime());
	}

	/**
	 * if no full job will auto open optimize job
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
		if (!breaker.isOn() && GlobalParam.TASK_COORDER.setFlowStatus(pipePump.getInstanceID(),L1seq,GlobalParam.JOB_TYPE.FULL.name(),
				STATUS.Ready,STATUS.Running,pipePump.getInstanceConfig().getPipeParams().showInfoLog())) {
			try { 
				String storeId=null;
				if (writeInSamePosition) {
					storeId = GlobalParam.TASK_COORDER.getStoreId(pipePump.getInstanceConfig().getPipeParams().getInstanceName(), 
							L1seq, pipePump.getID(), false, false);					
				} else {
					storeId = GlobalParam.TASK_COORDER.getStoreId(pipePump.getInstanceID(), L1seq, pipePump.getID(), false, false);
					CPU.RUN(pipePump.getID(), "Pond", "createStorePosition", true,
							Common.getInstanceRunId(pipePump.getInstanceID(), L1seq), storeId);
				}
				if(storeId!=null) {
					GlobalParam.TASK_COORDER.scanPositionkeepCurrentPos(pipePump.getInstanceID());
					pipePump.run(storeId, L1seq, true,writeInSamePosition);
					GlobalParam.TASK_COORDER.scanPositionRecoverKeep(pipePump.getInstanceID()); 
					GlobalParam.TASK_COORDER.saveTaskInfo(pipePump.getInstanceID(), L1seq, storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
				} 
			} catch (Exception e) {
				breaker.log();
				log.error(pipePump.getInstanceID() + " Full Exception", e);
			} finally {
				GlobalParam.TASK_COORDER.setFlowStatus(pipePump.getInstanceID(),L1seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Blank,STATUS.Ready,
						pipePump.getInstanceConfig().getPipeParams().showInfoLog());
			}
		} else {
			if(pipePump.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(pipePump.getInstanceID() + " current start full computational task has been breaked!");
		}
	}

	/**
	 * Primary virtual node full task running
	 */
	public void runMasterFull() {
		if (GlobalParam.TASK_COORDER.setFlowStatus(pipePump.getInstanceID(),L1seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Ready,STATUS.Running,
				pipePump.getInstanceConfig().getPipeParams().showInfoLog())) {
			try {
				String storeId;
				String destination = pipePump.getInstanceID();
				if(writeInSamePosition) {
					destination = pipePump.getInstanceConfig().getPipeParams().getInstanceName(); 
				} 
				storeId = GlobalParam.TASK_COORDER.getStoreId(destination, L1seq, pipePump.getID(), false, false); 

				CPU.RUN(pipePump.getID(), "Pond", "createStorePosition", true,
						Common.getInstanceRunId(pipePump.getInstanceID(), L1seq), storeId);
				GlobalParam.TASK_COORDER.setFlowInfo(destination,GlobalParam.JOB_TYPE.MASTER.name(),GlobalParam.FLOWINFO.FULL_JOBS.name(),
						getNextJobs(pipePump.getInstanceConfig().getPipeParams().getNextJob()));	
				for (String slave : pipePump.getInstanceConfig().getPipeParams().getNextJob()) { 
					Resource.FlOW_CENTER.runInstanceNow(slave, "full",pipePump.getInstanceConfig().getPipeParams().isAsync());
				}
			} catch (Exception e) {
				log.error(pipePump.getInstanceID() + " Full Exception", e);
			} finally {
				GlobalParam.TASK_COORDER.setFlowStatus(pipePump.getInstanceID(),L1seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Blank,STATUS.Ready,
						pipePump.getInstanceConfig().getPipeParams().showInfoLog());
			}
		} else {
			if(pipePump.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(pipePump.getInstanceID() + " Current Master Full flow has been breaked!");
		}
	}
	
	/**
	 * Primary virtual node Increment task running
	 * @throws EFException 
	 */
	public void runMasterIncrement() throws EFException {
		if (GlobalParam.TASK_COORDER.setFlowStatus(pipePump.getInstanceID(),L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Ready,STATUS.Running,
				pipePump.getInstanceConfig().getPipeParams().showInfoLog())) {
			try {
				String storeId = GlobalParam.TASK_COORDER.getStoreId(pipePump.getInstanceID(), L1seq, pipePump.getID(), true, recompute); 
				String destination = pipePump.getInstanceID();
				if(writeInSamePosition)
					destination = pipePump.getInstanceConfig().getPipeParams().getInstanceName();				
				GlobalParam.TASK_COORDER.setFlowInfo(destination, GlobalParam.JOB_TYPE.MASTER.name(), GlobalParam.FLOWINFO.INCRE_STOREID.name(), storeId); 				
				for (String slave : pipePump.getInstanceConfig().getPipeParams().getNextJob()) {
					Resource.FlOW_CENTER.runInstanceNow(slave, "increment",pipePump.getInstanceConfig().getPipeParams().isAsync());
				}
			} finally {
				GlobalParam.TASK_COORDER.setFlowStatus(pipePump.getInstanceID(),L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Blank,STATUS.Ready,
						pipePump.getInstanceConfig().getPipeParams().showInfoLog());  
				recompute = false;
			}
		} else {
			if(pipePump.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(pipePump.getInstanceID() + " Current Master Increment flow has been breaked!");
		}
	}

	/**
	 * slave instance increment job
	 */
	public void runIncrement() {  
		if (!breaker.isOn() && GlobalParam.TASK_COORDER.setFlowStatus(pipePump.getInstanceID(),L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Ready,STATUS.Running,
				pipePump.getInstanceConfig().getPipeParams().showInfoLog())) {
			String storeId;
			if (writeInSamePosition) {				
				storeId = GlobalParam.TASK_COORDER.getStoreId(pipePump.getInstanceConfig().getPipeParams().getInstanceName()
						, L1seq, pipePump.getID(), true, false);
				GlobalParam.TASK_COORDER.setAndGetScanInfo(pipePump.getInstanceID(), L1seq, storeId);
			} else {
				storeId = GlobalParam.TASK_COORDER.getStoreId(pipePump.getInstanceID(), L1seq, pipePump.getID(), true, recompute);
			} 
			try {
				pipePump.run(storeId, L1seq, false, writeInSamePosition); 
			} catch (EFException e) {
				if (!writeInSamePosition && e.getErrorType()==ETYPE.WRITE_POS_NOT_FOUND) { 
					storeId = GlobalParam.TASK_COORDER.getStoreId(pipePump.getInstanceID(), L1seq, pipePump.getID(), true, true);
					log.warn("try to rebuild {} storage location！",pipePump.getInstanceID());
					try {
						pipePump.run(storeId,L1seq, false, writeInSamePosition);
					} catch (EFException ex) {
						log.error("try to rebuild {} storage location exception,", pipePump.getInstanceID(),ex);
					}
				}else if (e.getErrorType()==ETYPE.EXTINTERRUPT){
					log.warn("{} increment external interrupt!",pipePump.getInstanceID());
				}else {
					breaker.log();
					log.error(pipePump.getInstanceID() + " IncrementJob Exception", e);
				}				
			} finally {
				recompute = this.checkReCompute(storeId);
				GlobalParam.TASK_COORDER.setFlowStatus(pipePump.getInstanceID(),L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Blank,STATUS.Ready,
						pipePump.getInstanceConfig().getPipeParams().showInfoLog()); 
			}
		} else {
			if(pipePump.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(pipePump.getInstanceID() + " The last round is not over, and the new increment is aborted!");
		}
	}
	
	
	/**
	 * It is mainly used to verify whether the new write position 
	 * needs to be switched under the time write mechanism
	 * @return
	 */
	private boolean checkReCompute(String storeId) {
		if(pipePump.getInstanceConfig().getPipeParams().getWriteMechanism()==MECHANISM.Time) {
			EFTuple<Long, Long> dTuple = EFWriterUtil.timeMechanism(pipePump.getInstanceConfig());
			if(!String.valueOf(dTuple.v1).equals(storeId)) {
				return true;
			}
		}
		return false;
	}
	
	private static String getNextJobs(String[] nextJobs) {
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
