/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.connection.EFConnectionSocket;
import org.elasticflow.config.NodeConfig;
import org.elasticflow.model.EFState;
import org.elasticflow.node.FlowCenter;
import org.elasticflow.node.NodeMonitor;
import org.elasticflow.node.SocketCenter;
import org.elasticflow.node.startup.Run;
import org.elasticflow.notifier.EFNotifier;
import org.elasticflow.service.HttpReaderService;
import org.elasticflow.service.SearcherService;
import org.elasticflow.task.job.TaskJobCenter;
import org.elasticflow.task.mode.FlowTask;
import org.quartz.Scheduler;

import com.alibaba.fastjson.JSONObject;

/**
 * Statistics current node resources
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-13 10:53
 */
public final class Resource {

	public static SocketCenter socketCenter;

	public static FlowCenter flowCenter;

	public static NodeMonitor nodeMonitor;

	public static TaskJobCenter taskJobCenter;

	public static Scheduler scheduler;

	public static EFNotifier EfNotifier;

	public static volatile NodeConfig nodeConfig;

	public static Run EFLOWS;

	/**flow running progress information */
	public final static EFState<HashMap<String, Object>> flowProgress = new EFState<HashMap<String, Object>>();
	
	/**flow process data position information*/
	public final static ConcurrentHashMap<String, JSONObject> flowStates = new ConcurrentHashMap<>();
	
	private final static ConcurrentHashMap<String, AtomicInteger> nodeErrorStates = new ConcurrentHashMap<String, AtomicInteger>(){ 
		private static final long serialVersionUID = 4065859783610995291L; 
	{
        put(ELEVEL.Ignore.name(), new AtomicInteger(0));
        put(ELEVEL.Dispose.name(), new AtomicInteger(0));
        put(ELEVEL.BreakOff.name(), new AtomicInteger(0));
        put(ELEVEL.Termination.name(), new AtomicInteger(0));
        put(ELEVEL.Stop.name(), new AtomicInteger(0));
    }};	
	
	/**warehouse resource status ;instance->JSONObject*/
	public final static HashMap<String, JSONObject> resourceStates = new HashMap<>();

	public volatile static HashMap<String, EFConnectionSocket<?>> EFConns = new HashMap<>();

	public static ConcurrentHashMap<String, FlowTask> tasks;

	public static ThreadPools threadPools;
	
	public static SearcherService searcherService;

	public static HttpReaderService httpReaderService;
	
	public static void incrementErrorStates(ELEVEL elevel) {
		nodeErrorStates.get(elevel.name()).incrementAndGet();
	}
	
	public static Integer getErrorStates(ELEVEL elevel) {
		return nodeErrorStates.get(elevel.name()).get();
	}
	
	public static void resetErrorStates() {
		nodeErrorStates.get(ELEVEL.Ignore.name()).set(0);
		nodeErrorStates.get(ELEVEL.Dispose.name()).set(0);
		nodeErrorStates.get(ELEVEL.BreakOff.name()).set(0);
		nodeErrorStates.get(ELEVEL.Termination.name()).set(0);
	}
}
