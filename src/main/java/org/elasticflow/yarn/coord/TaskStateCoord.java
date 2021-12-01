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
import org.elasticflow.model.Task;
import org.elasticflow.model.reader.ScanPosition;
import org.elasticflow.piper.PipePump;
import org.elasticflow.util.EFException;

/**
 * Running task status cluster coordination interface
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public interface TaskStateCoord extends Coordination{
	
	public void setFlowStatus(String instance, String L1seq,String tag,AtomicInteger ai);
	
	public void setScanPosition(Task task,String scanStamp) ;
	
	public boolean checkFlowStatus(String instance,String seq,JOB_TYPE type,STATUS state);
	
	public boolean setFlowStatus(String instance,String L1seq,String type,STATUS needState, STATUS setState,boolean showLog);
	
	public String getStoreId(String instance, String L1seq,boolean reload);
	
	public String getIncrementStoreId(String instance, String L1seq, PipePump transDataFlow,boolean reCompute) throws EFException;
	
	public void saveTaskInfo(String instance, String L1seq,String storeId,String location);
	
	public void setAndGetScanInfo(String instance, String L1seq,String storeId);
	
	public String getStoreId(String instance, String L1seq, PipePump transDataFlow, boolean isIncrement,
			boolean reCompute);
	
	public void scanPositionkeepCurrentPos(String instance);
	
	public void scanPositionRecoverKeep(String instance);
	
	public String getscanPositionString(String instance);
	
	public void putScanPosition(String instance,ScanPosition scanPosition);
	
	public String getLSeqPos(String instance,String L1seq,String L2seq);
	
	public void updateLSeqPos(String instance,String L1seq,String L2seq,String position);
	
	public void batchUpdateSeqPos(String instance,String val);	 
	
	public String getStoreId(String instance);
	
	public void setFlowInfo(String formKeyVal1,String formKeyVal2,String key,String data);
	
	public void resetFlowInfo(String formKeyVal1,String formKeyVal2);
	
	public HashMap<String, String> getFlowInfo(String formKeyVal1,String formKeyVal2);
	
}
