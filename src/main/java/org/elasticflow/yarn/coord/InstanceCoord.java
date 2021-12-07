/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coord;

/**
 * Run task instance cluster coordination interface
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public interface InstanceCoord extends Coordination{
	
	public void sendData(String content, String destination);
	
	public void sendInstanceData(String content0,String content1, String instance);
	
	public void addInstance(String instanceSettting);
	
	public void stopInstance(String instance,String jobtype);
	
	public void resumeInstance(String instance,String jobtype);
	
	public void removeInstance(String instance);
	
	public void updateNode(String ip, Integer nodeId);
	
	public void removeNode(String ip, Integer nodeId,boolean rebalace);
	
	public void stopNodes();
	
	public void clusterScan();
	
}
