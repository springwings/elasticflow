/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coord;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.model.reader.ScanPosition;
import org.elasticflow.util.EFException;

import com.alibaba.fastjson.JSONObject;

/**
 * Running task status cluster coordination interface
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public interface TaskStateCoord extends Coordination {

	public String getContextId(String instance, String L1seq, String tag);

	public void setFlowStatus(String instance, String L1seq, String tag, AtomicInteger ai);

	public void setScanPosition(String instance, String L1seq, String L2seq, String scanStamp, boolean reset,
			boolean isfull);

	public boolean checkFlowStatus(String instance, String seq, JOB_TYPE type, STATUS state);

	public boolean setFlowStatus(String instance, String L1seq, String type, STATUS needState, STATUS setState,
			boolean showLog);

	public String getStoreIdFromSave(String instance, String L1seq, boolean reload, boolean isfull);

	public String getIncrementStoreId(String instance, String L1seq, String contextId, boolean reCompute)
			throws EFException;

	public void saveTaskInfo(String instance, String L1seq, String storeId, boolean isfull);

	public void setAndGetScanInfo(String instance, String L1seq, String storeId, boolean isfull);

	public String getStoreId(String instance, String L1seq, String contextId, boolean isIncrement, boolean reCompute);

	public String getNewStoreId(String contextId, String instance, String L1seq, boolean isIncrement)
			throws EFException;

	public void scanPositionkeepCurrentPos(String instance);

	public void scanPositionRecoverKeep(String instance);

	public JSONObject getInstanceScanDatas(String instance, boolean isfull);

	// local run method
	public void initTaskDatas(String instance, ScanPosition scanPosition);

	public String getScanPositon(String instance, String L1seq, String L2seq, boolean isfull);

	public void setScanPositon(String instance, String L1seq, String L2seq, String position, boolean isfull);

	public void batchUpdateSeqPos(String instance, String val, boolean isfull);

	public String getStoreId(String instance, boolean isfull);

	public void setFlowInfo(String formKeyVal1, String formKeyVal2, String key, String data);

	public void resetFlowInfo(String formKeyVal1, String formKeyVal2);

	public HashMap<String, String> getFlowInfo(String formKeyVal1, String formKeyVal2);

	public void updateStoreData(String instance, Object data);

	public Object getStoreData(String instance);

}
