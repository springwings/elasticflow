/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.writer;

import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.MECHANISM;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.flow.Flow;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.piper.PipePump;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.instance.EFTuple;
import org.elasticflow.util.instance.EFWriterUtil;
import org.elasticflow.writer.handler.WriterHandler;
import org.elasticflow.yarn.Resource;
 
/**
 * Flow into Pond Manage
 * @author chengwen
 * @version 4.0
 * @date 2018-11-14 16:54
 * 
 */
@NotThreadSafe
public abstract class WriterFlowSocket extends Flow{
	
	/** defined custom write flow handler */
	protected WriterHandler writeHandler;
	
	/**batch submit documents*/
	protected Boolean isBatch = true;   
	
	@Override
	public void INIT(ConnectParams connectParams) {
		this.connectParams = connectParams;
		this.poolName = connectParams.getWhp().getPoolName(connectParams.getL1Seq());
		this.isBatch = GlobalParam.WRITE_BATCH; 
	}   
	
	public void setWriteHandler(WriterHandler writeHandler) {
		this.writeHandler = writeHandler;
	}
	
	public WriterHandler getWriteHandler() {
		return writeHandler;
	}	
	
	public String getNewStoreId(String mainName, boolean isIncrement, InstanceConfig instanceConfig) throws EFException{
		if(instanceConfig.getPipeParams().getWriteMechanism()==MECHANISM.AB) {
			return abMechanism(mainName,isIncrement,instanceConfig);
		}else if(instanceConfig.getPipeParams().getWriteMechanism()==MECHANISM.Time) {
			return timeMechanism(mainName,isIncrement,instanceConfig);
		}else {
			return normMechanism(mainName, isIncrement, instanceConfig);
		}
	}
	
	protected String normMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) throws EFException{ 
		String iName = Common.getStoreName(mainName, "");		
		if(this.storePositionExists(iName)==false) {
			this.create(mainName, "", instanceConfig);
			if(isIncrement==true)
				this.setAlias(mainName, "", instanceConfig.getAlias());
		}			
		return "";
	}
	
	/**
	 * Get the time-stamp of 0 o'clock every day/month
	 * Maintain expired instances
	 * @param mainName
	 * @param isIncrement
	 * @param instanceConfig
	 * @return time-stamp of second
	 */
	protected String timeMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) throws EFException{
		EFTuple<Long, Long> dTuple = EFWriterUtil.timeMechanism(instanceConfig); 
		String iName = Common.getStoreName(mainName, String.valueOf(dTuple.v2));
		try {			
			//remove out of date instance ,but this function not support cross L1Seqs destination		
			PipePump pipePump = Resource.SOCKET_CENTER.getPipePump(mainName,this.connectParams.getL1Seq(), 
					false,GlobalParam.FLOW_TAG._DEFAULT.name());
			pipePump.getWriter(dTuple.v2).removeInstance(mainName, String.valueOf(dTuple.v2));
		} catch (Exception e) {
			Common.LOG.error("remove instance　"+iName+" Exception!", e);
		}	 		
		if(this.storePositionExists(Common.getStoreName(mainName, String.valueOf(dTuple.v1)))==false) {
			this.create(mainName, String.valueOf(dTuple.v1), instanceConfig);
			if(isIncrement==true)
				this.setAlias(mainName, String.valueOf(dTuple.v1), instanceConfig.getAlias());
		}			
		return String.valueOf(dTuple.v1);
	}
	
	/**Create storage instance*/
	public abstract boolean create(String mainName, String storeId, InstanceConfig instanceConfig) throws EFException;
	
	public abstract boolean storePositionExists(String storeName);
	
	/**write one row data **/
	public abstract void write(WriterParam writerParam,PipeDataUnit unit,Map<String, EFField> transParams,String instance, String storeId,boolean isUpdate) throws EFException;
	
	/**Delete a single record through the key id*/ 
	public abstract void delete(String instance, String storeId,String keyColumn,String keyVal) throws EFException;
  
	public abstract void removeInstance(String instance, String storeId);
	
	protected abstract String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) throws EFException;
	
	public abstract void setAlias(String instance, String storeId, String aliasName); 
	
	/**
	 * Transaction confirmation
	 */
	public void flush() throws EFException{
		
	}

	public abstract void optimize(String instance, String storeId);
}
