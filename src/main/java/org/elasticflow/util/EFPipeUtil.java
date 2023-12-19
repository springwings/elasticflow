package org.elasticflow.util;

import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.yarn.Resource;
import org.quartz.SchedulerException;

/**
 * system pipe control
 * 
 * @author chengwen
 * @version 1.0
 * @date 2019-01-15 11:07
 * @modify 2019-01-15 11:07
 */
public class EFPipeUtil {

	public static boolean removeInstance(String instanceID, boolean removeTask, boolean removePipe) {
		Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
		boolean state = true;
		if (configMap.containsKey(instanceID)) {
			try {
				InstanceConfig instanceConfig = configMap.get(instanceID);
				String[] L1seqs = TaskUtil.getL1seqs(instanceConfig);
				for (String L1seq : L1seqs) {
					if (L1seq == null)
						continue;
					if (removeTask && Resource.tasks.containsKey(TaskUtil.getInstanceProcessId(instanceID, L1seq))) {
						Resource.tasks.remove(TaskUtil.getInstanceProcessId(instanceID, L1seq));
						state = removeFlowScheduleJob(TaskUtil.getInstanceProcessId(instanceID, L1seq), instanceConfig)
								&& state;
					}

					if (removePipe) {
						for (GlobalParam.FLOW_TAG tag : GlobalParam.FLOW_TAG.values()) {
							Resource.socketCenter.clearPipePump(instanceID, L1seq, tag.name());
						}
					}
				}
			} catch (Exception e) {
				Common.LOG.error("remove instance {} (remove removeTask {},remove removePipe {}) exception", instanceID,
						removeTask, removePipe, e);
				return false;
			}
			configMap.remove(instanceID);
		}
		return state;
	}

	public static boolean jobAction(String instanceID, String type, String actype) {
		String jobname = getJobName(instanceID, type);
		boolean state = false;
		switch (actype) {
		case "stop":
			state = Resource.taskJobCenter.stopJob(jobname);
			break;
		case "start":
			state = Resource.taskJobCenter.startNow(jobname);
			break;
		case "resume":
			state = Resource.taskJobCenter.restartJob(jobname);
			break;
		case "remove":
			state = Resource.taskJobCenter.deleteJob(jobname);
			break;
		}
		if (state) {
			Common.LOG.info("success " + actype + " task " + jobname);
		} else {
			Common.LOG.info("fail " + actype + " task " + jobname);
		}
		return state;
	}

	public static boolean removeFlowScheduleJob(String instanceID, InstanceConfig instanceConfig)
			throws SchedulerException {
		boolean state = true;
		if (instanceConfig.getPipeParams().getFullCron() != null) {
			jobAction(instanceID, GlobalParam.JOB_TYPE.FULL.name(), "stop");
			state = jobAction(instanceID, GlobalParam.JOB_TYPE.FULL.name(), "remove") && state;
		}
		if (instanceConfig.getPipeParams().getFullCron() == null
				|| instanceConfig.getPipeParams().getOptimizeCron() != null) {
			jobAction(instanceID, GlobalParam.JOB_TYPE.OPTIMIZE.name(), "stop");
			state = jobAction(instanceID, GlobalParam.JOB_TYPE.OPTIMIZE.name(), "remove") && state;
		}
		if (instanceConfig.getPipeParams().getDeltaCron() != null) {
			jobAction(instanceID, GlobalParam.JOB_TYPE.INCREMENT.name(), "stop");
			state = jobAction(instanceID, GlobalParam.JOB_TYPE.INCREMENT.name(), "remove") && state;
		}
		return state;
	}

	public static String getJobName(String instanceID, String type) {
		return instanceID + "_" + type;
	}
}
