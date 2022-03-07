package org.elasticflow.util;

import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.yarn.Resource;
import org.quartz.SchedulerException;

/**
 * system pipe control
 * @author chengwen
 * @version 1.0
 * @date 2019-01-15 11:07
 * @modify 2019-01-15 11:07
 */
public class EFPipeUtil {	
	
	public static boolean removeInstance(String instance,boolean removeTask,boolean removePipe){
		Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
		boolean state = true;
		if(configMap.containsKey(instance)){
			try{
				InstanceConfig instanceConfig = configMap.get(instance);
				String[] L1seqs = Common.getL1seqs(instanceConfig);
				for (String L1seq : L1seqs) {
					if (L1seq == null)
						continue;  
					if(removeTask && Resource.tasks.containsKey(Common.getInstanceRunId(instance, L1seq))) {
						Resource.tasks.remove(Common.getInstanceRunId(instance, L1seq));
						state = removeFlowScheduleJob(Common.getInstanceRunId(instance, L1seq),instanceConfig) && state;
					} 
					
					if(removePipe) {
						for(GlobalParam.FLOW_TAG tag:GlobalParam.FLOW_TAG.values()) {
							Resource.SOCKET_CENTER.clearPipePump(instance, L1seq, tag.name());
						} 
					}  
				}
			}catch(Exception e){
				Common.LOG.error("remove Instance "+instance+" Exception", e);
				return false;
			} 
			configMap.remove(instance);
		}
		return state;
	}
	
	public static boolean jobAction(String mainName, String type, String actype) {
		String jobname = getJobName(mainName, type);
		boolean state = false;
		switch (actype) {
		case "stop":
			state = Resource.taskJobCenter.stopJob(jobname);
			break;
		case "run":
			state = Resource.taskJobCenter.startNow(jobname);
			break;
		case "resume":
			state = Resource.taskJobCenter.restartJob(jobname);
			break;
		case "remove":
			state = Resource.taskJobCenter.deleteJob(jobname);
			break;
		}
		if(state){
			Common.LOG.info("Success " + actype + " Job " + jobname);
		}else{
			Common.LOG.info("Fail " + actype + " Job " + jobname);
		} 
		return state;
	} 
 
	public static boolean removeFlowScheduleJob(String instance,InstanceConfig instanceConfig)throws SchedulerException {
		boolean state = true;
		if (instanceConfig.getPipeParams().getFullCron() != null) { 
			jobAction(instance, GlobalParam.JOB_TYPE.FULL.name(), "stop");
			state = jobAction(instance, GlobalParam.JOB_TYPE.FULL.name(), "remove") && state;
		}
		if(instanceConfig.getPipeParams().getFullCron() == null || instanceConfig.getPipeParams().getOptimizeCron()!=null){
			jobAction(instance, GlobalParam.JOB_TYPE.OPTIMIZE.name(), "stop");
			state = jobAction(instance, GlobalParam.JOB_TYPE.OPTIMIZE.name(), "remove") && state;
		}
		if(instanceConfig.getPipeParams().getDeltaCron() != null){
			jobAction(instance, GlobalParam.JOB_TYPE.INCREMENT.name(), "stop");
			state = jobAction(instance, GlobalParam.JOB_TYPE.INCREMENT.name(), "remove") && state;
		}
		return state;
	}
	
	public static String getJobName(String instance, String type) { 
		return instance+"_"+type;
	}  
}
