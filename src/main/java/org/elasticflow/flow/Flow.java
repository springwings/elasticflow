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
import org.elasticflow.model.task.FlowStatistic;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.yarn.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EF data pipe flow model
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-31 10:52
 * @modify 2021-06-11 10:45
 */
public abstract class Flow {
	
	/**EF abstract connection*/
	protected volatile EFConnectionSocket<?> EFConn;
	
	/**database level */
	protected String L1seq;
	
	protected String poolName;

	protected InstanceConfig instanceConfig;
	
	/**EF abstract connection identification*/
	private String EFConnKey;
	
	/**Exclusive, i.e. independently built connections*/
	public boolean isConnMonopoly = false;
	
	/**Does the connection differentiate terminal usage? 
	 * If yes, it will be released directly without returning to the resource pool.
	 */
	public boolean isDiffEndType = false;

	public long lastGetPageTime = Common.getNow();

	public FlowStatistic flowStatistic;

	protected ConnectParams connectParams;
	
	/**Resource occupancy statistics lock*/
	protected AtomicInteger retainer = new AtomicInteger(0);

	private final static Logger log = LoggerFactory.getLogger("EF-Flow");

	public abstract void initConn(ConnectParams connectParams);

	public abstract void initFlow() throws EFException;
	
	/**if true,This instance task shares a connection with all subtasks under the same end, 
	 * 					mainly used to connect to scenarios with accompanying states, example: ES writer*/
	public static boolean crossSubtasks = false;
	
	/**
	 * 
	 * @param instanceConfig
	 * @param endType
	 * @param L1seq
	 * @param crossSubtasks 
	 * @throws EFException
	 */
	public void prepareFlow(InstanceConfig instanceConfig, END_TYPE endType, String L1seq) throws EFException {
		this.instanceConfig = instanceConfig;
		this.L1seq = L1seq;
		this.flowStatistic = new FlowStatistic(Resource.flowStates.get(instanceConfig.getInstanceID()), endType, L1seq);
		switch (endType) {
		case writer:
			this.EFConnKey = TaskUtil.getResourceTag(instanceConfig.getInstanceID(), L1seq, "",
					this.instanceConfig.getPipeParams().isWriterPoolShareAlias())+"_writer";
			break;
		case reader:
			this.EFConnKey = TaskUtil.getResourceTag(instanceConfig.getInstanceID(), L1seq, "",
					this.instanceConfig.getPipeParams().isReaderPoolShareAlias())+"_reader";
			break;
		default:
			this.EFConnKey = TaskUtil.getResourceTag(instanceConfig.getInstanceID(), L1seq, "",
					this.instanceConfig.getPipeParams().isSearcherShareAlias())+"_other";
			break;
		} 
		//When splitting an instance into multiple parallel subtasks, it can share connection resources with each other
		synchronized (Resource.EFConns) {
			if(crossSubtasks==false) {
				if(!this.EFConnKey.endsWith("_CROSS_RANDOM")) {
					this.EFConnKey = this.EFConnKey+Common.getNow()+"_CROSS_RANDOM";
					Resource.EFConns.put(this.EFConnKey, null);
				}
			}
			Resource.EFConns.put(this.EFConnKey, null);
		}
		this.initFlow();
	}
	
	
	/**
	 * release flow 
	 */
	public abstract void release(); 
	
	/**
	 * Enable exclusive resolution if the link has a special resource binding
	 * 
	 * @param isMonopoly      if true, the task will monopolize a specific
	 *                        connection and will not release it
	 * 						  Used in scenarios where connections cannot be mixed between different ends                       
	 * @param acceptShareConn if true, Use global shared connections
	 * 						  Share this connection globally 
	 * @return
	 */
	public synchronized EFConnectionSocket<?> PREPARE(boolean isMonopoly, boolean acceptShareConn) {
		if (isMonopoly) {
			if (this.EFConn == null) { 
				if (Resource.EFConns.get(this.EFConnKey) == null) {
					Resource.EFConns.put(this.EFConnKey,
							EFConnectionPool.getConn(this.connectParams, this.poolName, acceptShareConn));
				}
				this.EFConn = Resource.EFConns.get(this.EFConnKey);
			}
		} else {
			if (this.retainer.getAndIncrement() == 0) {
				Resource.EFConns.put(this.EFConnKey,
						EFConnectionPool.getConn(this.connectParams, this.poolName, acceptShareConn));
				this.EFConn = Resource.EFConns.get(this.EFConnKey);
			}
		}
		return this.EFConn;
	}

	public InstanceConfig getInstanceConfig() {
		return this.instanceConfig;
	}

	
	/**
	 * Release connection  resources
	 * @param isMonopoly   Belongs to exclusive nature and does not require maintenance
	 * @param releaseConn  forced release
	 */
	public void releaseConn(boolean isMonopoly, boolean clearConn) {
		if (isMonopoly == false || clearConn) {
			synchronized (this) {
				if (clearConn) {
					retainer.set(0);
				}
				if (retainer.decrementAndGet() <= 0) {
					EFConnectionPool.freeConn(this.EFConn, this.poolName, clearConn);
					this.EFConn = null;
					Resource.EFConns.put(this.EFConnKey, null);
					retainer.set(0);
				} else {
					log.info("connection {} retainer:{}",this.EFConn,retainer.get());
				}
			}
		}
	}
	
	

	public EFConnectionSocket<?> GETSOCKET() {
		return this.EFConn;
	}

	public boolean connStatus() {
		if (this.EFConn == null)
			return false;
		return true;
	}

	public void clearPool() {
		releaseConn(false,true);
		EFConnectionPool.clearPool(this.poolName);
	}
}
