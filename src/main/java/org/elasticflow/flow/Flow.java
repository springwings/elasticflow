/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.flow;

import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.connection.EFConnectionSocket;
import org.elasticflow.model.FlowState;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.yarn.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory; 
/**
 * EF data pipe flow model 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-31 10:52
 * @modify 2021-06-11 10:45
 */
public abstract class Flow {
	
	protected volatile EFConnectionSocket<?> EFConn;
	
	protected String poolName; 
	
	protected InstanceConfig instanceConfig;

	private int clearConnNum = 0; 
	
	public long lastGetPageTime = Common.getNow();
	
	public FlowState flowState;
	
	protected ConnectParams connectParams;
	
	protected AtomicInteger retainer = new AtomicInteger(0);
	
	private final static Logger log = LoggerFactory.getLogger("EF-Flow");
	
	public abstract void INIT(ConnectParams connectParams);
	
	/**
	 * Enable exclusive resolution if the link has a special resource binding
	 * @param isMonopoly  if true, the task will monopolize a specific connection and will not release it 
	 * @param acceptShareConn  if true, Use global shared connections
	 * @return
	 */
	public EFConnectionSocket<?> PREPARE(boolean isMonopoly,boolean acceptShareConn) {  
		synchronized (this.retainer) {  
			if(isMonopoly) {
				if(this.EFConn==null) 
					this.EFConn = EFConnectionPool.getConn(this.connectParams,
							this.poolName,acceptShareConn); 
			}else {
				if(this.retainer.getAndIncrement()==0) {
					this.EFConn = EFConnectionPool.getConn(this.connectParams,
							this.poolName,acceptShareConn);  
				} 
			}
		}
		return this.EFConn;
	}
	
	public void setInstanceConfig(InstanceConfig instanceConfig,END_TYPE endType) {
		this.instanceConfig = instanceConfig;		
		this.flowState = new FlowState(EFFileUtil.getInstancePath(instanceConfig.getName())[2],endType);
	}
	
	public InstanceConfig getInstanceConfig() {
		return this.instanceConfig;
	}
	
	private void stopTask() {
		clearConnNum+=1;
		if(clearConnNum>2) {
			clearConnNum = 0;
			log.warn("The resource is unstable. Try to stop the task of "+this.instanceConfig.getName());
			Resource.FlOW_CENTER.removeInstance(this.instanceConfig.getName(), true, true);
		}
	}
	
	public void REALEASE(boolean isMonopoly,boolean releaseConn) { 
		if(isMonopoly==false || releaseConn) { 
			synchronized(this.retainer){ 
				if(releaseConn) {
					this.stopTask();
					retainer.set(0);
				}					
				if(retainer.decrementAndGet()<=0){
					EFConnectionPool.freeConn(this.EFConn, this.poolName,releaseConn);  
					this.EFConn = null;
					retainer.set(0); 
				}else{
					log.info(this.EFConn+" retainer is "+retainer.get());
				}
			} 
		} 
	}   
	
	public EFConnectionSocket<?> GETSOCKET() {
		return this.EFConn;
	}
	
	public boolean ISLINK() {
		if(this.EFConn==null) 
			return false;
		return true;
	}	
	 
	public void clearPool() {
		this.stopTask();
		EFConnectionPool.clearPool(this.poolName);
	}
}
