/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coordinator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.TASK_FLOW_SINGAL;
import org.elasticflow.model.EFState;
import org.elasticflow.model.reader.ScanPosition;
import org.elasticflow.model.task.FlowStatistic;
import org.elasticflow.node.CPU;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.util.instance.PipeUtil;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.TaskStateCoord;

import com.alibaba.fastjson.JSONObject;

/**
 * task status cluster Coordinator, 
 * Coordinate pipeline status and information
 * run standlone/cluster mode
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public class TaskStateCoordinator implements TaskStateCoord, Serializable {

	private static final long serialVersionUID = 6182329757414086104L;

	/** store increment and full task data position */
	private final static ConcurrentHashMap<String, ScanPosition> SCAN_POSITION = new ConcurrentHashMap<>();

	/** Store intermediate calculation result data **/
	private final static ConcurrentHashMap<String, Object> TMP_STORE_DATA = new ConcurrentHashMap<>();

	/** flow control signal */
	final static EFState<AtomicInteger> FLOW_CONTROL_SINGAL = new EFState<>();

	public void initFlowSingal(String instance, String L1seq) {
		FLOW_CONTROL_SINGAL.set(instance, L1seq, GlobalParam.JOB_TYPE.FULL.name(), new AtomicInteger(TASK_FLOW_SINGAL.Ready.getVal()));
		FLOW_CONTROL_SINGAL.set(instance, L1seq, GlobalParam.JOB_TYPE.INCREMENT.name(), new AtomicInteger(TASK_FLOW_SINGAL.Ready.getVal()));
		FLOW_CONTROL_SINGAL.set(instance, L1seq, GlobalParam.JOB_TYPE.VIRTUAL.name(), new AtomicInteger(TASK_FLOW_SINGAL.Ready.getVal())); 
	}
	
	public boolean checkFlowSingal(String instance, String seq, JOB_TYPE type, TASK_FLOW_SINGAL singal) {
		if ((FLOW_CONTROL_SINGAL.get(instance, seq, type.name()).get() & singal.getVal()) > 0)
			return true;
		return false;
	}

	/**
	 * 
	 * @param instance
	 * @param seq
	 * @param type        tag for flow status,with job_type
	 * @param needState   equal 0 no need check
	 * @param plusState
	 * @param removeState
	 * @return boolean,lock status
	 */
	public boolean setFlowSingal(String instance, String L1seq, String type, TASK_FLOW_SINGAL needState,
			TASK_FLOW_SINGAL setState, boolean showLog) {
		synchronized (FLOW_CONTROL_SINGAL.get(instance, L1seq, type)) {
			if (needState.equals(TASK_FLOW_SINGAL.Blank)
					|| (FLOW_CONTROL_SINGAL.get(instance, L1seq, type).get() == needState.getVal())) {
				FLOW_CONTROL_SINGAL.get(instance, L1seq, type).set(setState.getVal());
				return true;
			} else {
				if (showLog)
					Common.LOG.info("{} {} flow can not set to {} state!", instance, type, needState.name());
				return false;
			}
		}
	}


	public String getContextId(String instance, String L1seq, String tag) {
		return Resource.socketCenter.getContextId(instance, L1seq, tag);
	} 

	public void updateStoreData(String instance, Object data) {
		synchronized (TMP_STORE_DATA.get(instance)) {
			TMP_STORE_DATA.put(instance, data);
		}
	}

	public Object getStoreData(String instance) {
		return TMP_STORE_DATA.get(instance);
	}

	public void setScanPosition(String instance, String L1seq, String L2seq, String scanStamp, boolean reset,
			boolean isfull) {
		if (reset || PipeUtil.scanPosCompare(scanStamp,
				SCAN_POSITION.get(instance).getLSeqPos(TaskUtil.getLseq(L1seq, L2seq), isfull))) {
			SCAN_POSITION.get(instance).updateLSeqPos(TaskUtil.getLseq(L1seq, L2seq), scanStamp, isfull);
			// update flow status,Distributed environment synchronization status
			if (GlobalParam.DISTRIBUTE_RUN) {
				Resource.flowStates.get(instance).put(FlowStatistic.getStoreKey(L1seq),
						GlobalParam.INSTANCE_COORDER.distributeCoorder().getPipeEndStatus(instance, L1seq));
			} else {
				Resource.flowStates.get(instance).put(FlowStatistic.getStoreKey(L1seq),
						EFMonitorUtil.getPipeEndStatus(instance, L1seq));
			}
			EFFileUtil.createAndSave(Resource.flowStates.get(instance).toJSONString(),
					EFFileUtil.getInstancePath(instance)[2]);
		}
	}


	/**
	 * Retrieve storage ID from disk storage
	 */
	public String getStoreIdFromSave(String instance, String L1seq, boolean reload, boolean isfull) {
		if (reload) {
			String path = TaskUtil.getInstanceStorePath(instance,
					isfull ? GlobalParam.JOB_FULLINFO_PATH : GlobalParam.JOB_INCREMENTINFO_PATH);
			ScanPosition sp = new ScanPosition(instance, GlobalParam.DEFAULT_RESOURCE_SEQ);
			synchronized (SCAN_POSITION) {
				byte[] b = EFDataStorer.getData(path, true);
				if (b != null && b.length > 0) {
					String str = new String(b);
					try {
						sp.loadInfos(str, isfull);
					} catch (Exception e) {
						Common.LOG.error("instance {} L1seq {},try to parse {} exception", instance, L1seq, path, e);
					}
				}
				SCAN_POSITION.put(instance, sp);
			}
		}
		return SCAN_POSITION.get(instance).getStoreId(isfull);
	}

	public String getIncrementStoreId(String instance, String L1seq, String contextId, boolean reCompute)
			throws EFException {
		String storeId = getStoreIdFromSave(instance, L1seq, true, false);
		if (storeId.length() == 0 || reCompute) {
			storeId = getNewStoreId(contextId, instance, L1seq, true);
			if (storeId == null)
				storeId = "a";
			this.saveTaskInfo(instance, L1seq, storeId, false);
		}
		return storeId;
	}

	public String getNewStoreId(String contextId, String instance, String L1seq, boolean isIncrement)
			throws EFException {
		return (String) CPU.RUN(contextId, "Pond", "getNewStoreId", false,
				TaskUtil.getInstanceProcessId(instance, L1seq), isIncrement);
	}

	/**
	 * write task state to file
	 * 
	 * @param instance
	 * @param L1seq
	 * @param storeId
	 * @param isfull
	 */
	public void saveTaskInfo(String instance, String L1seq, String storeId, boolean isfull) {
		synchronized (SCAN_POSITION) {
			SCAN_POSITION.get(instance).updateStoreId(storeId, isfull);
			EFDataStorer.setData(
					TaskUtil.getInstanceStorePath(instance,
							isfull ? GlobalParam.JOB_FULLINFO_PATH : GlobalParam.JOB_INCREMENTINFO_PATH),
					SCAN_POSITION.get(instance).getString(isfull));
		}
	}

	/**
	 * get increment store tag name and will auto create new one with some
	 * conditions.
	 * 
	 * @param isIncrement
	 * @param reCompute   force to get storeid recompute from destination engine
	 * @param L1seq       for series data source sequence
	 * @param instance    data source main tag name
	 * @return String
	 * @throws EFException
	 */
	public String getStoreId(String instance, String L1seq, String contextId, boolean isIncrement, boolean reCompute)
			throws EFException {
		if (isIncrement) {
			return getIncrementStoreId(instance, L1seq, contextId, reCompute);
		} else {
			return getNewStoreId(contextId, instance, L1seq, false);
		}
	}

	public void scanPositionkeepCurrentPos(String instance) {
		SCAN_POSITION.get(instance).keepCurrentPos();
	}

	public void scanPositionRecoverKeep(String instance) {
		SCAN_POSITION.get(instance).recoverKeep();
	}

	public JSONObject getInstanceScanDatas(String instance, boolean isfull) {
		return SCAN_POSITION.get(instance).getPositionDatas(isfull);
	}

	// local run method
	public synchronized void initTaskDatas(String instance, ScanPosition scanPosition) {
		SCAN_POSITION.put(instance, scanPosition);
		TMP_STORE_DATA.put(instance, "");
	}

	public String getScanPositon(String instance, String L1seq, String L2seq, boolean isfull) {
		return SCAN_POSITION.get(instance).getLSeqPos(TaskUtil.getLseq(L1seq, L2seq), isfull);
	}

	public void setScanPositon(String instance, String L1seq, String L2seq, String position, boolean isfull) {
		synchronized (SCAN_POSITION.get(instance)) {
			SCAN_POSITION.get(instance).updateLSeqPos(TaskUtil.getLseq(L1seq, L2seq), position, isfull);
		}
	}

	public void batchUpdateSeqPos(String instance, String val, boolean isfull) {
		SCAN_POSITION.get(instance).batchUpdateSeqPos(val, isfull);
	}

	/**
	 * from SCAN_POSITION get store id
	 * 
	 * @param instance
	 * @return
	 */
	public String getStoreId(String instance, boolean isfull) {
		return SCAN_POSITION.get(instance).getStoreId(isfull);
	}
 
	public void initFlowProgressInfo(String instanceID) {
		Resource.flowProgress.set(instanceID, JOB_TYPE.FULL.name(), new HashMap<String, Object>());
		Resource.flowProgress.set(instanceID, JOB_TYPE.INCREMENT.name(), new HashMap<String, Object>());
		Resource.flowProgress.set(instanceID, JOB_TYPE.VIRTUAL.name(), new HashMap<String, Object>());
		Resource.flowProgress.set(instanceID, JOB_TYPE.INSTRUCTION.name(), new HashMap<String, Object>());
	}
	
	public void setFlowProgressInfo(String instanceID, String jobType, String key, Object data) { 
		Resource.flowProgress.get(instanceID, jobType).put(key, data);
	}
	
	public void resetFlowProgressInfo(String instanceID, String jobType) {
		Resource.flowProgress.get(instanceID, jobType).clear();
	}

	public HashMap<String, Object> getFlowInfo(String instanceID, String jobType) {
		return Resource.flowProgress.get(instanceID, jobType);
	}
}
