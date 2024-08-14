/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.writer;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.MECHANISM;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.flow.Flow;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.piper.PipePump;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.instance.EFTuple;
import org.elasticflow.util.instance.EFWriterUtil;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.writer.handler.WriterHandler;
import org.elasticflow.yarn.Resource;

/**
 * Writer Flow Socket
 * 
 * @author chengwen
 * @version 4.0
 * @date 2018-11-14 16:54
 * 
 */
@NotThreadSafe
public abstract class WriterFlowSocket extends Flow {

	/** defined custom write flow handler */
	protected WriterHandler writeHandler;

	/** batch submit documents */
	protected Boolean isBatch = true;

	@Override
	public void initConn(ConnectParams connectParams) {
		this.connectParams = connectParams;
		this.poolName = this.connectParams.getWhp().getPoolName(this.connectParams.getL1Seq()); 
		this.isBatch = GlobalParam.WRITE_BATCH;
	}

	@Override
	public void initFlow() {
		// auto invoke in flow prepare
	}
	
	
	/**
	 * release computer flow
	 */
	@Override
	public void release() {
		if(this.writeHandler!=null)
			this.writeHandler.release();
		releaseConn(isConnMonopoly,isDiffEndType,crossSubtasks); 
	}

	public void setWriteHandler(WriterHandler writeHandler) {
		this.writeHandler = writeHandler;
	}

	public WriterHandler getWriteHandler() {
		return writeHandler;
	}

	/**
	 * Get storage additional ID
	 * @param mainName   instanceID
	 * @param isIncrement
	 * @param instanceConfig
	 * @return
	 * @throws EFException
	 */
	public String getNewStoreId(String mainName, boolean isIncrement, InstanceConfig instanceConfig)
			throws EFException {
		if (instanceConfig.getPipeParams().getWriteMechanism() == MECHANISM.AB) {
			return abMechanism(mainName, isIncrement, instanceConfig);
		} else if (instanceConfig.getPipeParams().getWriteMechanism() == MECHANISM.Time) {
			return timeMechanism(mainName, isIncrement, instanceConfig);
		} else {
			return normMechanism(mainName, isIncrement, instanceConfig);
		}
	}

	protected String normMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig)
			throws EFException {
		String iName = TaskUtil.getStoreName(mainName, "");
		if (this.storePositionExists(iName) == false) {
			this.create(mainName, "", instanceConfig);
			if (isIncrement == true && !mainName.equals(instanceConfig.getAlias()))
				this.setAlias(mainName, "", instanceConfig.getAlias());
		}
		return "";
	}

	/**
	 * Get the time-stamp of 0 o'clock every day/month Maintain expired instances
	 * 
	 * @param mainName
	 * @param isIncrement
	 * @param instanceConfig
	 * @return time-stamp of second
	 */
	protected String timeMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig)
			throws EFException {
		EFTuple<Long, Long> dTuple = EFWriterUtil.timeMechanism(instanceConfig);
		String storeName = TaskUtil.getStoreName(mainName, String.valueOf(dTuple.v2));
		try {
			// remove out of date instance ,but this function not support cross L1Seqs
			// destination
			PipePump pipePump = Resource.socketCenter.getPipePump(mainName, this.connectParams.getL1Seq(), false,
					GlobalParam.FLOW_TAG._DEFAULT.name());
			pipePump.getWriter(dTuple.v2).removeShard(mainName, String.valueOf(dTuple.v2));
		} catch (Exception e) {
			Common.systemLog("time-Mechanism try to remove instance {} exception", storeName,e);
		}
		if (this.storePositionExists(TaskUtil.getStoreName(mainName, String.valueOf(dTuple.v1))) == false) {
			this.create(mainName, String.valueOf(dTuple.v1), instanceConfig);
			if (isIncrement == true)
				this.setAlias(mainName, String.valueOf(dTuple.v1), instanceConfig.getAlias());
		}
		return String.valueOf(dTuple.v1);
	}
	
	/**A/B switching write mechanism*/
	protected abstract String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig)
			throws EFException;

	/** Create storage instance */
	public abstract boolean create(String mainName, String storeId, InstanceConfig instanceConfig) throws EFException;
	
	/**Determine whether there are instance storage tables or indexes*/
	public abstract boolean storePositionExists(String storeName) throws EFException;

	/** write one row data **/
	public abstract void write(InstanceConfig instanceConfig, PipeDataUnit unit, String instance, String storeId,
			boolean isUpdate) throws EFException;

	/** Delete a single record through the key id */
	public abstract void delete(String instance, String storeId, String keyColumn, String keyVal) throws EFException;
	
	/**Delete shard data*/
	public abstract void removeShard(String instance, String storeId) throws EFException;
	
	/**Set instance alias (used for naming across multiple instances)*/
	public abstract void setAlias(String instance, String storeId, String aliasName) throws EFException;

	/**
	 * Transaction confirmation
	 */
	public void flush() throws EFException {}
	
	/**Optimize instance storage or index class information*/
	public abstract void optimize(String instance, String storeId);
}
