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
	private boolean isReferenceInstance = false;
	private PipePump pipePump;
	private Breaker breaker;
	/**
	 * seq for scan series datas
	 */
	private String L1seq = "";
	
	/**write real destination*/
	private String destination;
	private String instanceId;
 
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
		breaker.init(pipePump.getInstanceConfig().getInstanceID(),
				pipePump.getInstanceConfig().getPipeParams().getFailFreq(),
				pipePump.getInstanceConfig().getPipeParams().getMaxFailTime());
		destination = getDestination();
		instanceId = pipePump.getInstanceID();
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
		if (!breaker.isOn() && GlobalParam.TASK_COORDER.setFlowStatus(instanceId,L1seq,GlobalParam.JOB_TYPE.FULL.name(),
				STATUS.Ready,STATUS.Running,pipePump.getInstanceConfig().getPipeParams().showInfoLog())) {
			try { 
				String storeId = GlobalParam.TASK_COORDER.getStoreId(destination, L1seq, pipePump.getID(), false, false);
				if (!isReferenceInstance) 
					CPU.RUN(pipePump.getID(), "Pond", "createStorePosition", true,
							Common.getInstanceRunId(destination, L1seq), storeId);
				if(storeId!=null) {
					GlobalParam.TASK_COORDER.scanPositionkeepCurrentPos(instanceId);
					pipePump.run(storeId, L1seq, true,isReferenceInstance);
					GlobalParam.TASK_COORDER.scanPositionRecoverKeep(instanceId); 
					GlobalParam.TASK_COORDER.saveTaskInfo(instanceId, L1seq, storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
				} 
				for (String slave : pipePump.getInstanceConfig().getPipeParams().getNextJob()) { 
					Resource.FlOW_CENTER.runInstanceNow(slave, "full",pipePump.getInstanceConfig().getPipeParams().isAsync());
				}
			} catch (Exception e) {
				breaker.log();
				log.error(instanceId + " Full Exception", e);
			} finally {
				GlobalParam.TASK_COORDER.setFlowStatus(instanceId,L1seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Blank,STATUS.Ready,
						pipePump.getInstanceConfig().getPipeParams().showInfoLog());
			}
		} else {
			if(pipePump.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(instanceId + " current start full computational task has been breaked!");
		}
	} 

	/**
	 * Primary virtual node full task running
	 */
	public void runVirtualFull() {
		if (GlobalParam.TASK_COORDER.setFlowStatus(instanceId,L1seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Ready,STATUS.Running,
				pipePump.getInstanceConfig().getPipeParams().showInfoLog())) {			
			try {
				String storeId = GlobalParam.TASK_COORDER.getStoreId(destination, L1seq, pipePump.getID(), false, false); 
				CPU.RUN(pipePump.getID(), "Pond", "createStorePosition", true,Common.getInstanceRunId(destination, L1seq), storeId);
				GlobalParam.TASK_COORDER.setFlowInfo(instanceId,GlobalParam.JOB_TYPE.VIRTUAL.name(),GlobalParam.FLOWINFO.FULL_JOBS.name(),
						getNextJobs(pipePump.getInstanceConfig().getPipeParams().getNextJob()));	
				for (String slave : pipePump.getInstanceConfig().getPipeParams().getNextJob()) { 
					Resource.FlOW_CENTER.runInstanceNow(slave, "full",pipePump.getInstanceConfig().getPipeParams().isAsync());
				}
			} catch (Exception e) {
				log.error(instanceId + " Virtual Full Exception", e);
			} finally {
				GlobalParam.TASK_COORDER.setFlowStatus(instanceId,L1seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Blank,STATUS.Ready,
						pipePump.getInstanceConfig().getPipeParams().showInfoLog());
			}
		} else {
			if(pipePump.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(instanceId + " Current Virtual Full flow has been breaked!");
		}
	}
	
	/**
	 * Primary virtual node Increment task running
	 * @throws EFException 
	 */
	public void runVirtualIncrement() {
		if (GlobalParam.TASK_COORDER.setFlowStatus(instanceId,L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Ready,STATUS.Running,
				pipePump.getInstanceConfig().getPipeParams().showInfoLog())) {
			try {				
				String storeId = GlobalParam.TASK_COORDER.getStoreId(destination, L1seq, pipePump.getID(), true, recompute); 						
				GlobalParam.TASK_COORDER.setFlowInfo(instanceId, GlobalParam.JOB_TYPE.VIRTUAL.name(), GlobalParam.FLOWINFO.INCRE_STOREID.name(), storeId); 				
				for (String slave : pipePump.getInstanceConfig().getPipeParams().getNextJob()) {
					Resource.FlOW_CENTER.runInstanceNow(slave, "increment",pipePump.getInstanceConfig().getPipeParams().isAsync());
				}
			} catch (Exception e) {
				log.error(instanceId + " Virtual Increment Exception", e);
			}finally {
				GlobalParam.TASK_COORDER.setFlowStatus(instanceId,L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Blank,STATUS.Ready,
						pipePump.getInstanceConfig().getPipeParams().showInfoLog());  
				recompute = false;
			}
		} else {
			if(pipePump.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(instanceId + " Current Master Increment flow has been breaked!");
		}
	}

	/**
	 * slave instance increment job
	 */
	public void runIncrement() {  
		if (!breaker.isOn() && GlobalParam.TASK_COORDER.setFlowStatus(instanceId,L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Ready,STATUS.Running,
				pipePump.getInstanceConfig().getPipeParams().showInfoLog())) { 
			String storeId = GlobalParam.TASK_COORDER.getStoreId(destination, L1seq, pipePump.getID(), true, (isReferenceInstance ? false : recompute));
			GlobalParam.TASK_COORDER.setAndGetScanInfo(instanceId, L1seq, storeId);				
			try {
				pipePump.run(storeId, L1seq, false, isReferenceInstance); 
				for (String slave : pipePump.getInstanceConfig().getPipeParams().getNextJob()) {
					Resource.FlOW_CENTER.runInstanceNow(slave, "increment",pipePump.getInstanceConfig().getPipeParams().isAsync());
				}
			} catch (EFException e) {
				if (!isReferenceInstance && e.getErrorType()==ETYPE.WRITE_POS_NOT_FOUND) { 
					storeId = GlobalParam.TASK_COORDER.getStoreId(destination, L1seq, pipePump.getID(), true, true);
					log.warn("try to rebuild {} storage location！",destination);
					try {
						pipePump.run(storeId,L1seq, false, isReferenceInstance);
					} catch (EFException ex) {
						log.error("try to rebuild {} storage location exception,", destination,ex);
					}
				}else if (e.getErrorType()==ETYPE.EXTINTERRUPT){
					log.warn("{} increment external interrupt!",instanceId);
				}else {
					breaker.log();
					log.error(instanceId + " IncrementJob Exception,{}",e.getTrack(),e);
				}				
			} finally {
				recompute = this.checkReCompute(storeId);
				GlobalParam.TASK_COORDER.setFlowStatus(instanceId,L1seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Blank,STATUS.Ready,
						pipePump.getInstanceConfig().getPipeParams().showInfoLog()); 
			}
		} else {
			if(pipePump.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(instanceId + " The last round is not over, and the new increment is aborted!");
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
	
	/**
	 * 
	 * @param isFull
	 * @return destination
	 */
	private String getDestination() {
		String destination;
		if(isReferenceInstance) {
			destination = pipePump.getInstanceConfig().getPipeParams().getReferenceInstance(); 
		}else {
			destination = pipePump.getInstanceID();
		}
		return destination;
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
