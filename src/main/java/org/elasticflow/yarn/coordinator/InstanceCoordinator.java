/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coordinator;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.util.EFPipeUtil;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.InstanceCoord;

import com.alibaba.fastjson.JSONObject;

/**
 * Run task instance cluster coordination operation 
 * The code runs on the slave/master,local running method 
 * @author chengwen
 * @version 0.1
 * @create_time 2019-07-30
 */
public class InstanceCoordinator implements InstanceCoord { 
	
	//master control
	private DistributeCoorder distributeCoorder;
	
	public InstanceCoordinator() {
		if(!EFNodeUtil.isSlave())
			distributeCoorder = new DistributeCoorder();
	}
	
	@Override
	public DistributeCoorder distributeCoorder() {
		return this.distributeCoorder;
	}
	
	@Override
	public void initNode(boolean isOnStart) {
		boolean wait = isOnStart;
		while(Resource.tasks.size()>0) {
			EFMonitorUtil.cleanAllInstance(wait);
			wait = false;
		}			
	}
	
	@Override
	public void updateInstanceConfig(String instance,String end,String fieldName,String value) {
		InstanceConfig tmp = Resource.nodeConfig.getInstanceConfigs().get(instance);
		try {
			Class<?> cls = null;
			Object obj = null;
			switch(end) {
			case "TransParam":
				cls = tmp.getPipeParams().getClass();
				obj = tmp.getPipeParams();
				break;
			case "ReadParam":
				cls = tmp.getReadParams().getClass();
				obj = tmp.getReadParams();
				break;
			case "ComputeParam":
				cls = tmp.getComputeParams().getClass();
				obj = tmp.getComputeParams();
				break;
			case "WriteParam":
				cls = tmp.getWriterParams().getClass();
				obj = tmp.getWriterParams();
				break;	
			}  
			Common.setConfigObj(obj, cls, fieldName,value);
		} catch (Exception e) {
			Common.LOG.error("update Instance Config Exception",e);
		}	
	}
	
	@Override
	public int onlineTasksNum() {
		return Resource.tasks.size();
	}
	
	@Override
	public void sendData(String content, String destination,boolean relative) {
		if(relative) {
			EFFileUtil.createAndSave(content, GlobalParam.CONFIG_PATH + destination);
		}else {
			EFFileUtil.createAndSave(content, destination);
		}		
	}

	@Override
	public void reloadResource() {
		Resource.nodeConfig.parsePondFile(GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("pond"));
		Resource.nodeConfig.parseInstructionsFile(GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("instructions"));
	}
	
	@Override
	public void sendInstanceData(String content0, String content1,String content2,String instance) {
		String[] paths = EFFileUtil.getInstancePath(instance);
		sendData(content0, paths[0],false);
		sendData(content1, paths[1],false);
		sendData(content2, paths[2],false);
	}

	@Override
	public void addInstance(String instanceSettting,boolean createSchedule) {
		Resource.nodeConfig.loadConfig(instanceSettting, false);
		Resource.nodeConfig.loadInstanceConfig(instanceSettting);
		String tmp[] = instanceSettting.split(":");
		String instanceName = tmp[0];
		InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instanceName);
		if (instanceConfig.checkStatus())
			EFNodeUtil.loadInstanceDatas(instanceConfig);
		EFMonitorUtil.rebuildFlowGovern(instanceSettting, createSchedule);
	} 

	@Override
	public void stopInstance(String instance, String jobtype) {
		if (jobtype.toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Stop, false);
		} else {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Stop, true);
		}
	}

	@Override
	public void resumeInstance(String instance, String jobtype) {
		if (jobtype.toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Ready, false);
		} else {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Ready, true);
		}
	}

	@Override
	public void removeInstance(String instance,boolean waitComplete) {
		if(waitComplete)
			EFMonitorUtil.controlInstanceState(instance, STATUS.Stop, true);
		if (Resource.nodeConfig.getInstanceConfigs().get(instance).getInstanceType() > 0) {
			Resource.flowInfos.remove(instance, JOB_TYPE.FULL.name());
			Resource.flowInfos.remove(instance, JOB_TYPE.INCREMENT.name());
		}		
		EFPipeUtil.removeInstance(instance, true, true);
		EFMonitorUtil.removeConfigInstance(instance);
	} 
	
	@Override
	public boolean runInstanceNow(String instance,String type,boolean asyn) {
		return Resource.flowCenter.runInstanceNow(instance, type, asyn);
	}
	
	@Override
	public void resetBreaker(String instance,String L1seq) {
		Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.reset();
	}
	
	@Override
	public JSONObject getBreakerStatus(String instance,String L1seq,String appendPipe) {
		JSONObject JO = new JSONObject();
		JO.put(appendPipe + "breaker_is_on", Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.isOn());
		if(Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.isOn()) {
			JO.put(appendPipe + "breaker_is_on_reason", Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.getReason());
		}
		JO.put(appendPipe + "valve_turn_level", Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).valve.getTurnLevel());
		JO.put(appendPipe + "current_fail_freq", Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.failInterval());
		JO.put(appendPipe + "total_fail_times", Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.getFailTimes());
		return JO;
	}	 
	
}
