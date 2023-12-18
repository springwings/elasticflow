/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.node;

import java.util.HashSet;
import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.TASK_STATUS;
import org.elasticflow.model.task.TaskJobModel;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.param.pipe.InstructionParam;
import org.elasticflow.piper.PipePump;
import org.elasticflow.task.mode.FlowTask;
import org.elasticflow.task.mode.InstructionTask;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFPipeUtil;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.yarn.Resource;
import org.quartz.SchedulerException;

import com.alibaba.fastjson.JSONObject;

/**
 * read to write flow build center
 * @author chengwen
 * @version 2.0 
 */
public class FlowCenter{ 
		
	private String default_cron = "0 PARAM 01 * * ?";
	
	private String not_run_cron = "0 0 0 1 1 ? 2099";
	
	private HashSet<String> cron_exists=new HashSet<String>();
 
	
	public void startInstructionsJob() {
		Map<String, InstructionParam> instructions = Resource.nodeConfig.getInstructions();
		for (Map.Entry<String,InstructionParam> entry : instructions.entrySet()){
			createInstructionScheduleJob(entry.getValue(),InstructionTask.createTask(entry.getKey()));
		}
	}
	
	/**
	 * 
	 * @param instance
	 * @param type
	 * @param asyn control job in master is running in asynchronous or not?
	 * @return
	 */
	public boolean runInstanceNow(String instance,String type,boolean asyn){ 
		InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instance); 
		boolean state = true; 
		try {
			if (instanceConfig.openTrans() == false) {
				Common.LOG.info("instance {} data exchange service not enabled!",instance);
				return false;   
			} 
			String[] L1seqs = TaskUtil.getL1seqs(instanceConfig);  
			for (String L1seq : L1seqs) {
				if (L1seq == null)
					continue;
				
				if(GlobalParam.JOB_TYPE.FULL.name().equals(type.toUpperCase())) {
					if (GlobalParam.TASK_COORDER.checkFlowStatus(instance, L1seq,GlobalParam.JOB_TYPE.FULL,TASK_STATUS.Ready))
						state = EFPipeUtil.jobAction(TaskUtil.getInstanceProcessId(instance, L1seq), GlobalParam.JOB_TYPE.FULL.name(), "start") && state;
						if(state && !asyn) {
							Thread.sleep(500);//waiting to start job
							while(GlobalParam.TASK_COORDER.checkFlowStatus(instance, L1seq,GlobalParam.JOB_TYPE.FULL,TASK_STATUS.Ready)==false)
								Thread.sleep(500);
						} 
				}else {
					if (GlobalParam.TASK_COORDER.checkFlowStatus(instance, L1seq,GlobalParam.JOB_TYPE.INCREMENT,TASK_STATUS.Ready))
						state = EFPipeUtil.jobAction(TaskUtil.getInstanceProcessId(instance, L1seq), GlobalParam.JOB_TYPE.INCREMENT.name(), "start") && state;
						if(state && asyn) {
							Thread.sleep(500);//waiting to start job
							while(GlobalParam.TASK_COORDER.checkFlowStatus(instance, L1seq,GlobalParam.JOB_TYPE.INCREMENT,TASK_STATUS.Ready)==false)
								Thread.sleep(500);
						} 
				}				
			}
		} catch (Exception e) {
			Common.LOG.error("run instance now "+instance+" Exception", e);
			return false;
		}

		return state;
	}
 
	
	
	/**
	 * Add flow pipeline management
	 * @param instanceName
	 * @param instanceConfig
	 * @param needClear
	 * @param createSchedule
	 * @param contextId
	 * @throws EFException 
	 */
	public void addFlowGovern(String instanceID, InstanceConfig instanceConfig,boolean needClear,boolean createSchedule) throws EFException { 
		if (instanceConfig.checkStatus()==false || instanceConfig.openTrans() == false)
			return; 
		try {
			String[] L1seqs = TaskUtil.getL1seqs(instanceConfig);  
			if(!Resource.flowStates.containsKey(instanceID)) {
				String content = EFFileUtil.readText(EFFileUtil.getInstancePath(instanceID)[2], GlobalParam.ENCODING, true);
				if (content!=null && content.length()>0) {
					Resource.flowStates.put(instanceID, JSONObject.parseObject(content));
				}else {
					Resource.flowStates.put(instanceID,new JSONObject());
				}
			}
			for (String L1seq : L1seqs) {
				if (L1seq == null)
					continue; 
				if(!Resource.tasks.containsKey(TaskUtil.getInstanceProcessId(instanceID, L1seq)) || needClear){
					PipePump pipePump = Resource.socketCenter.getPipePump(instanceID, L1seq,needClear,GlobalParam.FLOW_TAG._DEFAULT.name());
					Resource.tasks.put(TaskUtil.getInstanceProcessId(instanceID, L1seq), FlowTask.createTask(pipePump, L1seq));
				}  
				if(createSchedule)
					createFlowScheduleJob(TaskUtil.getInstanceProcessId(instanceID, L1seq), Resource.tasks.get(TaskUtil.getInstanceProcessId(instanceID, L1seq)),
						instanceConfig,needClear);
			}
		} catch (Exception e) {
			Common.LOG.error("Add "+instanceID+" Flow Govern Exception", e);
		} 
	}

	
	private void createInstructionScheduleJob(InstructionParam param, InstructionTask task) {
		TaskJobModel _sj = new TaskJobModel(param.getId(),
				EFPipeUtil.getJobName(param.getId(), GlobalParam.JOB_TYPE.INSTRUCTION.name()), param.getCron(),
				"org.elasticflow.task.InstructionTask", "runInstructions", task); 
		try {
			Resource.taskJobCenter.addJob(_sj); 
		}catch (Exception e) {
			Common.LOG.error("create Instruction Job "+param.getId()+" Exception", e);
		} 
	}

	private void createFlowScheduleJob(String instanceID, FlowTask task,
			InstanceConfig instanceConfig,boolean needclear)
			throws SchedulerException {
		String fullFun="runFull";
		String incrementFun="runIncrement";
		if(instanceConfig.getPipeParams().isVirtualPipe()) {
			fullFun="runVirtualFull";
			incrementFun="runVirtualIncrement";
		} 
		
		if (instanceConfig.getPipeParams().getFullCron() != null) { 
			if(needclear)
				EFPipeUtil.jobAction(instanceID, GlobalParam.JOB_TYPE.FULL.name(), "remove"); 
			TaskJobModel _sj = new TaskJobModel(instanceID,
					EFPipeUtil.getJobName(instanceID, GlobalParam.JOB_TYPE.FULL.name()), instanceConfig.getPipeParams().getFullCron(),
					"org.elasticflow.task.FlowTask", fullFun, task); 
			Resource.taskJobCenter.addJob(_sj); 
		}else if(instanceConfig.getPipeParams().getReadFrom()!= null && instanceConfig.getPipeParams().getWriteTo()!=null) { 
			instanceConfig.setHasFullJob(false);
			if(needclear)
				EFPipeUtil.jobAction(instanceID, GlobalParam.JOB_TYPE.FULL.name(), "remove");
			//Add valid full tasks to prevent other program errors
			TaskJobModel _sj = new TaskJobModel(instanceID,
					EFPipeUtil.getJobName(instanceID,GlobalParam.JOB_TYPE.FULL.name()), not_run_cron,
					"org.elasticflow.task.FlowTask", fullFun, task); 
			Resource.taskJobCenter.addJob(_sj); 
		}  
		
		if (instanceConfig.getPipeParams().getDeltaCron() != null) { 
			if(needclear)
				EFPipeUtil.jobAction(instanceID, GlobalParam.JOB_TYPE.INCREMENT.name(), "remove");
			
			String cron = instanceConfig.getPipeParams().getDeltaCron();
			if(this.cron_exists.contains(cron)){
				String[] strs = cron.trim().split(" ");
				strs[0] = String.valueOf((int)(Math.random()*60));
				String _s="";
				for(String s:strs){
					_s+=s+" ";
				}
				cron = _s.trim();
			}else{
				this.cron_exists.add(cron);
			}
			TaskJobModel _sj = new TaskJobModel(instanceID,
					EFPipeUtil.getJobName(instanceID, GlobalParam.JOB_TYPE.INCREMENT.name()),
					instanceConfig.getPipeParams().getDeltaCron(), "org.elasticflow.task.FlowTask",
					incrementFun, task); 
			Resource.taskJobCenter.addJob(_sj);
		}else if(instanceConfig.getPipeParams().getReadFrom()!= null && instanceConfig.getPipeParams().getWriteTo()!=null) {
			if(needclear)
				EFPipeUtil.jobAction(instanceID, GlobalParam.JOB_TYPE.INCREMENT.name(), "remove");
			//Add valid full tasks to prevent other program errors
			TaskJobModel _sj = new TaskJobModel(instanceID,
					EFPipeUtil.getJobName(instanceID,GlobalParam.JOB_TYPE.INCREMENT.name()),
					not_run_cron, "org.elasticflow.task.FlowTask",
					incrementFun, task); 
			Resource.taskJobCenter.addJob(_sj);
		}
		
		if(instanceConfig.getPipeParams().getFullCron() == null || instanceConfig.getPipeParams().getOptimizeCron()!=null){
			if(needclear)
				EFPipeUtil.jobAction(instanceID,GlobalParam.JOB_TYPE.OPTIMIZE.name(), "remove");
		
			String cron = instanceConfig.getPipeParams().getOptimizeCron()==null?default_cron.replace("PARAM",String.valueOf((int)(Math.random()*60))):instanceConfig.getPipeParams().getOptimizeCron();
			instanceConfig.getPipeParams().setOptimizeCron(cron);
			if(instanceConfig.getPipeParams().getReferenceInstance()==null) 
				createOptimizeJob(instanceID, task,cron); 
		}
	}
	
	private void createOptimizeJob(String instanceID, FlowTask batch,String cron) throws SchedulerException{
		TaskJobModel _sj = new TaskJobModel(instanceID,
				EFPipeUtil.getJobName(instanceID, GlobalParam.JOB_TYPE.OPTIMIZE.name()),cron,
				"org.elasticflow.manager.Task", "optimizeInstance", batch); 
		Resource.taskJobCenter.addJob(_sj); 
	}


}
