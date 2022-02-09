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
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.model.EFState;
import org.elasticflow.model.reader.ScanPosition;
import org.elasticflow.node.CPU;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.util.instance.PipeUtil;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.TaskStateCoord;

/**
 * Running task status cluster Coordinator Coordinate pipeline status and
 * information
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public class TaskStateCoordinator implements TaskStateCoord, Serializable {

	private static final long serialVersionUID = 6182329757414086104L;

	private final static ConcurrentHashMap<String, ScanPosition> SCAN_POSITION = new ConcurrentHashMap<>();
	
	/**Store intermediate calculation result data**/
	private final static ConcurrentHashMap<String,Object> TMP_STORE_DATA = new ConcurrentHashMap<>();

	/** FLOW_STATUS store current flow running control status */
	final static EFState<AtomicInteger> FLOW_STATUS = new EFState<>();

	public String getContextId(String instance, String L1seq, String tag) {
		return Resource.SOCKET_CENTER.getContextId(instance, L1seq, tag);
	}

	public void setFlowStatus(String instance, String L1seq, String tag, AtomicInteger ai) {
		FLOW_STATUS.set(instance, L1seq, tag, ai);
	}
	
	public void updateStoreData(String instance,Object data) {
		synchronized (TMP_STORE_DATA.get(instance)) {
			TMP_STORE_DATA.put(instance, data);
		}
	}
	
	public Object getStoreData(String instance) {
		return TMP_STORE_DATA.get(instance);
	}

	public void setScanPosition(String instance, String L1seq, String L2seq, String scanStamp) {
		synchronized (SCAN_POSITION.get(instance)) {
			if (PipeUtil.scanPosCompare(scanStamp,
					SCAN_POSITION.get(instance).getLSeqPos(Common.getLseq(L1seq, L2seq)))) {
				SCAN_POSITION.get(instance).updateLSeqPos(Common.getLseq(L1seq, L2seq), scanStamp);
				//update flow status,Distributed environment synchronization status
				if(GlobalParam.DISTRIBUTE_RUN) {
					EFFileUtil.createAndSave(GlobalParam.INSTANCE_COORDER.getPipeEndStatus(instance, L1seq).toJSONString(), 
							EFFileUtil.getInstancePath(instance)[2]);
				} else {
					EFFileUtil.createAndSave(EFMonitorUtil.getPipeEndStatus(instance, L1seq).toJSONString(), 
							EFFileUtil.getInstancePath(instance)[2]);
				} 
				
			}
		}
	}

	public boolean checkFlowStatus(String instance, String seq, JOB_TYPE type, STATUS state) {
		if ((FLOW_STATUS.get(instance, seq, type.name()).get() & state.getVal()) > 0)
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
	public boolean setFlowStatus(String instance, String L1seq, String type, STATUS needState, STATUS setState,
			boolean showLog) {
		synchronized (FLOW_STATUS.get(instance, L1seq, type)) {
			if (needState.equals(STATUS.Blank)
					|| (FLOW_STATUS.get(instance, L1seq, type).get() == needState.getVal())) {
				FLOW_STATUS.get(instance, L1seq, type).set(setState.getVal());
				return true;
			} else {
				if (showLog)
					Common.LOG.info("{} {} not in {} state!", instance, type, needState.name());
				return false;
			}
		}
	}

	/**
	 * get store tag name from save file
	 * 
	 * @param instanceName
	 * @param L1seq
	 * @return String
	 */
	public String getStoreIdFromSave(String instance, String L1seq, boolean reload) {
		if (reload) { 
			String path = Common.getTaskStorePath(instance, L1seq, GlobalParam.JOB_INCREMENTINFO_PATH);
			byte[] b = EFDataStorer.getData(path, true);
			synchronized (SCAN_POSITION.get(instance)) {
				if (b != null && b.length > 0) {
					String str = new String(b);
					SCAN_POSITION.put(instance, new ScanPosition(str, instance, GlobalParam.DEFAULT_RESOURCE_SEQ));
				} else {
					SCAN_POSITION.put(instance, new ScanPosition(instance, GlobalParam.DEFAULT_RESOURCE_SEQ));
				}
			}			
		}
		return SCAN_POSITION.get(instance).getStoreId();
	}

	public String getIncrementStoreId(String instance, String L1seq, String contextId, boolean reCompute)
			throws EFException { 
		String storeId = getStoreIdFromSave(instance, L1seq, true);
		if (storeId.length() == 0 || reCompute) {
			storeId = getNewStoreId(contextId, instance, L1seq, true);
			if (storeId == null)
				storeId = "a";
			saveTaskInfo(instance, L1seq, storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
		}
		return storeId;
	}

	public String getNewStoreId(String contextId, String instance, String L1seq, boolean isIncrement)
			throws EFException {
		return (String) CPU.RUN(contextId, "Pond", "getNewStoreId", false, Common.getInstanceId(instance, L1seq),
				isIncrement);
	}

	/**
	 * 
	 * @param instance
	 * @param L1seq
	 * @param storeId
	 * @param location
	 */
	public void saveTaskInfo(String instance, String L1seq, String storeId, String location) {
		synchronized(SCAN_POSITION.get(instance)) {
			SCAN_POSITION.get(instance).updateStoreId(storeId);
		}
		EFDataStorer.setData(Common.getTaskStorePath(instance, L1seq, location),
				SCAN_POSITION.get(instance).getString());
	}

	/**
	 * for Master/slave job get and set LastUpdateTime
	 * 
	 * @param instance
	 * @param L1seq
	 * @param storeId  Master store id
	 */
	public void setAndGetScanInfo(String instance, String L1seq, String storeId) {
		String path = Common.getTaskStorePath(instance, L1seq, GlobalParam.JOB_INCREMENTINFO_PATH);
		byte[] b = EFDataStorer.getData(path, true);
		synchronized (SCAN_POSITION.get(instance)) {			
			if (b != null && b.length > 0) {
				String str = new String(b);
				SCAN_POSITION.put(instance, new ScanPosition(str, instance, storeId));
			} else {
				SCAN_POSITION.put(instance, new ScanPosition(instance, storeId));
			}
			saveTaskInfo(instance, L1seq, storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
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
	public String getStoreId(String instance, String L1seq, String contextId, boolean isIncrement, boolean reCompute) {
		try {
			if (isIncrement) {
				return getIncrementStoreId(instance, L1seq, contextId, reCompute);
			} else {
				return getNewStoreId(contextId, instance, L1seq, false);
			}
		} catch (EFException e) {
			Common.LOG.error("getStoreId", e);
			Resource.FlOW_CENTER.removeInstance(instance, true, true);
			Common.processErrorLevel(e);
		}
		return null;
	}

	public void scanPositionkeepCurrentPos(String instance) {
		SCAN_POSITION.get(instance).keepCurrentPos();
	}

	public void scanPositionRecoverKeep(String instance) {
		SCAN_POSITION.get(instance).recoverKeep();
	}

	public String getscanPositionString(String instance) {
		return SCAN_POSITION.get(instance).getPositionString();
	}

	public void initPutDatas(String instance, ScanPosition scanPosition) {
		synchronized(SCAN_POSITION) {
			SCAN_POSITION.put(instance, scanPosition);
			TMP_STORE_DATA.put(instance, "");
		}		
	}

	public String getLSeqPos(String instance, String L1seq, String L2seq) {
		return SCAN_POSITION.get(instance).getLSeqPos(Common.getLseq(L1seq, L2seq));
	}

	public void updateLSeqPos(String instance, String L1seq, String L2seq, String position) {
		synchronized(SCAN_POSITION.get(instance)) {
			SCAN_POSITION.get(instance).updateLSeqPos(Common.getLseq(L1seq, L2seq), position);
		}		
	}

	public void batchUpdateSeqPos(String instance, String val) {
		synchronized(SCAN_POSITION.get(instance)) {
			SCAN_POSITION.get(instance).batchUpdateSeqPos(val);
		}
	}

	/**
	 * from SCAN_POSITION get store id
	 * 
	 * @param instance
	 * @return
	 */
	public String getStoreId(String instance) {
		return SCAN_POSITION.get(instance).getStoreId();
	}

	public void setFlowInfo(String formKeyVal1, String formKeyVal2, String key, String data) {
		if (!Resource.FLOW_INFOS.containsKey(formKeyVal1, formKeyVal2))
			Resource.FLOW_INFOS.set(formKeyVal1, formKeyVal2, new HashMap<String, String>());
		Resource.FLOW_INFOS.get(formKeyVal1, formKeyVal2).put(key, data);
	}

	public void resetFlowInfo(String formKeyVal1, String formKeyVal2) {
		Resource.FLOW_INFOS.get(formKeyVal1, formKeyVal2).clear();
	}

	public HashMap<String, String> getFlowInfo(String formKeyVal1, String formKeyVal2) {
		return Resource.FLOW_INFOS.get(formKeyVal1, formKeyVal2);
	}
}
