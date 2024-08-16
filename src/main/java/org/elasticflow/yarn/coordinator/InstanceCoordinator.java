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
import org.elasticflow.config.GlobalParam.TASK_FLOW_SINGAL;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.util.EFPipeUtil;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.master.InstanceCoord;

import com.alibaba.fastjson.JSONObject;

/**
 * Run task instance cluster coordination operation 
 * DistributeCorder is exclusively used by the master node to control other machines, 
 * while other methods are used to control its own instances
 * @author chengwen
 * @version 0.1
 * @create_time 2019-07-30
 */
public class InstanceCoordinator implements InstanceCoord { 
	
	/**master control，Control remote slave node machines*/
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
	public void initNode() {
		//Direct cleanup of all resident tasks
		EFMonitorUtil.cleanAllInstance(false);			
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
				cls = tmp.getReaderParams().getClass();
				obj = tmp.getReaderParams();
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
			Common.setConfigObj(obj, cls, fieldName,value,null);
		} catch (Exception e) {
			Common.systemLog("update instance {} task config exception",instance,e);
		}	
	}
	
	@Override
	public int onlineTasksNum() {
		return Resource.tasks.size();
	}
	
	@Override
	public void sendData(String content, String destination,boolean relative) {
		if(relative) {
			EFFileUtil.createAndSave(content, GlobalParam.DATAS_CONFIG_PATH + destination);
		}else {
			EFFileUtil.createAndSave(content, destination);
		}		
	}

	@Override
	public void reloadResource() {
		Resource.nodeConfig.parsePondFile(GlobalParam.DATAS_CONFIG_PATH + "/" + GlobalParam.SystemConfig.getProperty("pond"));
		Resource.nodeConfig.parseInstructionsFile(GlobalParam.DATAS_CONFIG_PATH + "/" + GlobalParam.SystemConfig.getProperty("instructions"));
	}
	
	@Override
	public void sendInstanceData(String content0, String content1,String content2,String instance) {
		String[] paths = EFFileUtil.getInstancePath(instance);
		sendData(content0, paths[0],false);
		sendData(content1, paths[1],false);
		sendData(content2, paths[2],false);
	}
	
	@Override
	public boolean loadInstance(String instanceSettting,boolean createSchedule,boolean reset) {
		Resource.nodeConfig.loadConfig(instanceSettting, reset);
		String tmp[] = instanceSettting.split(":");
		String instanceName = tmp[0];
		InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instanceName);		
		try {
			if (instanceConfig.checkStatus())
				EFNodeUtil.loadInstanceDatas(instanceConfig);
			EFMonitorUtil.rebuildFlowGovern(instanceSettting, createSchedule);
			return true;
		} catch (EFException e) {
			Resource.nodeConfig.unloadInstanceConfig(instanceSettting);
			Common.LOG.error("load instance {} exception",instanceName,e);
		}
		return false;
	}

	@Override
	public void stopInstance(String instance, String jobtype) {
		if (jobtype.toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
			EFMonitorUtil.controlInstanceState(instance, TASK_FLOW_SINGAL.Stop, false);
		} else {
			EFMonitorUtil.controlInstanceState(instance, TASK_FLOW_SINGAL.Stop, true);
		}
	}

	@Override
	public void resumeInstance(String instance, String jobtype) {
		if (jobtype.toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
			EFMonitorUtil.controlInstanceState(instance, TASK_FLOW_SINGAL.Ready, false);
		} else {
			EFMonitorUtil.controlInstanceState(instance, TASK_FLOW_SINGAL.Ready, true);
		}
	}

	@Override
	public void removeInstance(String instance,boolean waitComplete) { 
		if(waitComplete)
			EFMonitorUtil.controlInstanceState(instance, TASK_FLOW_SINGAL.Stop, true);
		if (Resource.nodeConfig.getInstanceConfigs().containsKey(instance) &&
				Resource.nodeConfig.getInstanceConfigs().get(instance).getInstanceType() > 0) {
			if(Resource.flowProgress.containsKey(instance)) { 
				Resource.flowProgress.remove(instance, JOB_TYPE.FULL.name());
				Resource.flowProgress.remove(instance, JOB_TYPE.INCREMENT.name()); 
			}
		} 
		EFPipeUtil.removeInstance(instance, true, true);
	} 
	
	@Override
	public boolean runInstanceNow(String instance,String type,boolean asyn) {
		return Resource.flowCenter.runInstanceNow(instance, type, asyn);
	}
	
	@Override
	public void resetBreaker(String instance,String L1seq) {
		Resource.tasks.get(TaskUtil.getInstanceProcessId(instance, L1seq)).breaker.reset();
	}
	
	@Override
	public JSONObject getBreakerStatus(String instance,String L1seq,String appendPipe) {
		JSONObject JO = new JSONObject(); 
		String instanceId = TaskUtil.getInstanceProcessId(instance, L1seq);
		if(Resource.tasks.containsKey(instanceId)) {
			JO.put(appendPipe + "breaker_is_on", Resource.tasks.get(TaskUtil.getInstanceProcessId(instance, L1seq)).breaker.isOn());
			if(Resource.tasks.get(TaskUtil.getInstanceProcessId(instance, L1seq)).breaker.isOn()) {
				JO.put(appendPipe + "breaker_is_on_reason", Resource.tasks.get(TaskUtil.getInstanceProcessId(instance, L1seq)).breaker.getReason());
			}
			JO.put(appendPipe + "valve_turn_level", Resource.tasks.get(TaskUtil.getInstanceProcessId(instance, L1seq)).valve.getTurnLevel());
			JO.put(appendPipe + "current_fail_interval", Resource.tasks.get(TaskUtil.getInstanceProcessId(instance, L1seq)).breaker.failInterval());
			JO.put(appendPipe + "total_fail_times", Resource.tasks.get(TaskUtil.getInstanceProcessId(instance, L1seq)).breaker.getFailTimes());
		}else {
			JO.put(appendPipe + "breaker_is_on",false);
			JO.put(appendPipe + "valve_turn_level",9);
			JO.put(appendPipe + "current_fail_interval",Integer.MAX_VALUE);
			JO.put(appendPipe + "total_fail_times",0);
		}		
		return JO;
	}	
}
