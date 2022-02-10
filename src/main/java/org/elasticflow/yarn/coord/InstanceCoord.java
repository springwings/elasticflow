/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coord;

import java.util.Queue;

import com.alibaba.fastjson.JSONObject;

/**
 * Run task instance cluster coordination interface
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public interface InstanceCoord extends Coordination{
	
	public void initNode(boolean isOnStart);
	
	public void updateInstanceConfig(String instance,String end,String fieldName,String value);
	
	public void updateNodeConfigs(String instance,String end,String fieldName,String value);
	
	public void sendData(String content, String destination,boolean relative);
	
	public void reloadResource();
	
	public int onlineTasksNum();
	
	public String getConnectionStatus(String instance,String poolName);
	
	public JSONObject getPipeEndStatus(String instance,String L1seq);
	
	public void sendInstanceData(String content0,String content1,String content2, String instance);
	
	public void addInstance(String instanceSettting);
	
	public void stopInstance(String instance,String jobtype);
	
	public void resumeInstance(String instance,String jobtype);
	
	public void removeInstance(String instance,boolean waitComplete);
	
	public void updateNode(String ip, Integer nodeId);
	
	public void removeNode(String ip, Integer nodeId,boolean rebalace);
	
	public void stopNodes();
	
	public void updateAllNodesResource();
	
	public Queue<String> clusterScan(boolean startRebalace);
	
	public void pushInstanceToCluster(String instanceSettting);
	
	public void removeInstanceFromCluster(String instance);
	
}
