/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.config.NodeConfig;
import org.elasticflow.model.EFState;
import org.elasticflow.node.FlowCenter;
import org.elasticflow.node.NodeMonitor;
import org.elasticflow.node.SocketCenter;
import org.elasticflow.node.startup.Run;
import org.elasticflow.task.FlowTask;
import org.elasticflow.util.email.EFEmailSender;

/**
 * Statistics current node resources
 * @author chengwen
 * @version 1.0
 * @date 2018-11-13 10:53
 */
public final class Resource {

	public static SocketCenter SOCKET_CENTER;
	
	public static FlowCenter FlOW_CENTER;
	
	public static NodeMonitor nodeMonitor; 
	
	public static EFEmailSender mailSender; 
	
	public static NodeConfig nodeConfig;
	
	public static Run EFLOWS;
	/**FLOW_STATUS store current flow running control status*/
	public final static EFState<AtomicInteger> FLOW_STATUS = new EFState<>();
	/**FLOW_INFOS store current flow running state information*/
	public final static EFState<HashMap<String,String>> FLOW_INFOS = new EFState<HashMap<String,String>>();

	public static HashMap<String, FlowTask> tasks; 
	
	public final static ThreadPools ThreadPools = new ThreadPools();
	
	public void collectNodeResource() {
		
	}
	 
}
