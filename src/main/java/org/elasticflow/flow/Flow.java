/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.flow;

import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.connection.EFConnectionSocket;
import org.elasticflow.model.FlowState;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
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
	
	protected String L1seq;
	
	protected String poolName; 
	
	protected InstanceConfig instanceConfig;

	private int clearConnNum = 0; 
	
	private String EFConnKey;
	
	public long lastGetPageTime = Common.getNow();
	
	public FlowState flowState;
	
	protected ConnectParams connectParams;
	
	protected AtomicInteger retainer = new AtomicInteger(0);
	
	private final static Logger log = LoggerFactory.getLogger("EF-Flow");
	
	public abstract void initConn(ConnectParams connectParams);
	
	public abstract void initFlow();
	
	public void prepareFlow(InstanceConfig instanceConfig,END_TYPE endType,String L1seq) {
		this.instanceConfig = instanceConfig;
		this.L1seq = L1seq;
		this.flowState = new FlowState(Resource.FLOW_STAT.get(instanceConfig.getInstanceID()),endType,L1seq);
		switch(endType) {
		case writer:
			this.EFConnKey = Common.getResourceTag(instanceConfig.getInstanceID(), L1seq, "", this.instanceConfig.getPipeParams().isWriterPoolShareAlias());
			break;
		case reader:
			this.EFConnKey = Common.getResourceTag(instanceConfig.getInstanceID(), L1seq, "", this.instanceConfig.getPipeParams().isReaderPoolShareAlias());
			break;
		default:
			this.EFConnKey = Common.getResourceTag(instanceConfig.getInstanceID(), L1seq, "", this.instanceConfig.getPipeParams().isSearcherShareAlias());
			break;
		}
		synchronized(Resource.EFCONNS) {
			Resource.EFCONNS.put(this.EFConnKey, null);
		}
		this.initFlow();
	}
	
	/**
	 * Enable exclusive resolution if the link has a special resource binding
	 * @param isMonopoly  if true, the task will monopolize a specific connection and will not release it 
	 * @param acceptShareConn  if true, Use global shared connections
	 * @return
	 */
	public EFConnectionSocket<?> PREPARE(boolean isMonopoly,boolean acceptShareConn) {  
		synchronized (this.retainer) { 
			if(isMonopoly) { 
				if(this.EFConn==null) {
					if(Resource.EFCONNS.get(this.EFConnKey)==null) {
						Resource.EFCONNS.put(this.EFConnKey, EFConnectionPool.getConn(this.connectParams,
								this.poolName,acceptShareConn));
					}
					this.EFConn = Resource.EFCONNS.get(this.EFConnKey);
				}					
			}else {
				if(this.retainer.getAndIncrement()==0) {
					Resource.EFCONNS.put(this.EFConnKey, EFConnectionPool.getConn(this.connectParams,
							this.poolName,acceptShareConn));
					this.EFConn = Resource.EFCONNS.get(this.EFConnKey);
				}					
			}
		}		
		return this.EFConn;
	} 

	
	public InstanceConfig getInstanceConfig() {
		return this.instanceConfig;
	}
	
	private void stopTask() {
		clearConnNum+=1;
		if(clearConnNum>2) {
			clearConnNum = 0;
			log.warn("The resource is unstable. Try to stop the task of "+this.instanceConfig.getInstanceID());
			Resource.FlOW_CENTER.removeInstance(this.instanceConfig.getInstanceID(), true, true);
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
					try {
						EFConnectionPool.freeConn(this.EFConn, this.poolName,releaseConn);  
					}catch(Exception e) {
						log.error("freeConn exception",e);
					}					
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
