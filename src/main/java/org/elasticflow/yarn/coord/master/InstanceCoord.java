/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coord.master;

import org.elasticflow.yarn.coordinator.DistributeCoorder;

import com.alibaba.fastjson.JSONObject;

/**
 * Run task instance cluster coordination interface
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public interface InstanceCoord extends Coordination{
	
	public void initNode();
	
	public void updateInstanceConfig(String instance,String end,String fieldName,String value);
		
	public void sendData(String content, String destination,boolean relative);
	
	public void reloadResource();
	
	public int onlineTasksNum(); 
	/**
	 * send instance data to slave
	 * @param content0	increment job info
	 * @param content1	task.xml info
	 * @param content2	stat info
	 * @param instance
	 */
	public void sendInstanceData(String content0,String content1,String content2, String instance);
	
	public boolean loadInstance(String instanceSettting,boolean createSchedule,boolean reset);
	
	public void stopInstance(String instance,String jobtype);
	
	public boolean runInstanceNow(String instance,String type,boolean asyn);
		
	public void resumeInstance(String instance,String jobtype);
	
	public void removeInstance(String instance,boolean waitComplete);
	
	/**get distributeCoorder controller*/
	public DistributeCoorder distributeCoorder();
	
	/**Obtain circuit breaker status*/
	public JSONObject getBreakerStatus(String instance,String L1seq,String appendPipe);
	
	/**reset breaker status*/
	public void resetBreaker(String instance,String L1seq);
	
}
