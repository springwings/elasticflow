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
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.connection.EFConnectionSocket;
import org.elasticflow.param.pipe.ConnectParams;
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
	
	/** Average load of scheduled tasks,the amount of data processed per second**/
	protected long LOAD;
	
	/**Transient performance,the amount of data processed per second**/
	protected long PERFORMANCE;
	
	public long lastGetPageTime = System.currentTimeMillis();
	
	protected ConnectParams connectParams;
	
	protected AtomicInteger retainer = new AtomicInteger(0);
	
	private final static Logger log = LoggerFactory.getLogger(Flow.class);
	
	public abstract void INIT(ConnectParams connectParams);
	
	/**
	 * 
	 * @param isMonopoly  if true, the task will monopolize a specific connection and will not release it 
	 * @param canSharePipe  if true, Use global shared connections
	 * @return
	 */
	public EFConnectionSocket<?> PREPARE(boolean isMonopoly,boolean canSharePipe) {  
		synchronized (this.retainer) {  
			if(isMonopoly) {
				if(this.EFConn==null) 
					this.EFConn = EFConnectionPool.getConn(this.connectParams,
							this.poolName,canSharePipe); 
			}else {
				if(this.retainer.getAndIncrement()==0) {
					this.EFConn = EFConnectionPool.getConn(this.connectParams,
							this.poolName,canSharePipe);  
				} 
			}
		}
		return this.EFConn;
	}
	
	public void setInstanceConfig(InstanceConfig instanceConfig) {
		this.instanceConfig = instanceConfig;
	}
	
	public InstanceConfig getInstanceConfig() {
		return this.instanceConfig;
	}
	
	public void REALEASE(boolean isMonopoly,boolean releaseConn) { 
		if(isMonopoly==false) { 
			synchronized(retainer){ 
				if(releaseConn)
					retainer.set(0);
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
	
	public long getLoad() {
		return this.LOAD;
	}
	
	public long getPerformance() {
		return this.PERFORMANCE;
	}
	
	public void setLoad(long load) {
		this.LOAD = load;;
	}
	
	public void setPerformance(long performance) {
		this.PERFORMANCE = performance;
	}
	
	public void clearPool() {
		EFConnectionPool.clearPool(this.poolName);
	}
}
