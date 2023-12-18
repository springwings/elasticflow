package org.elasticflow.task.job;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.model.task.TaskJobModel;
import org.elasticflow.util.Common;
import org.elasticflow.yarn.Resource;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;

/**
 * shedule job manager for add,remove,stop,restart,start once manager
 * 
 * @author chengwen
 * @version 1.0
 */
public class TaskJobCenter {

	public boolean addJob(TaskJobModel taskjob) throws SchedulerException {
		if (taskjob == null) {
			Common.LOG.error("task is null!");
			return false;
		}

		TriggerKey triggerKey = TriggerKey.triggerKey(taskjob.getJobName());
		CronTrigger trigger = (CronTrigger) Resource.scheduler.getTrigger(triggerKey);

		if (trigger == null) {
			Common.LOG.info("success add task {}", taskjob.getJobName());
			JobDetail jobDetail = JobBuilder.newJob(JobRunFactory.class).withIdentity(taskjob.getJobName()).build();
			jobDetail.getJobDataMap().put(GlobalParam.FLOW_TAG._DEFAULT.name(), taskjob);
			CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(taskjob.getCron());
			trigger = TriggerBuilder.newTrigger().withIdentity(taskjob.getJobName(), taskjob.getJobName())
					.withSchedule(scheduleBuilder).build();
			Resource.scheduler.scheduleJob(jobDetail, trigger);
		} else {
			Common.LOG.info("success modify task {}", taskjob.getJobName());
			CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(taskjob.getCron());
			trigger = trigger.getTriggerBuilder().withIdentity(triggerKey).withSchedule(scheduleBuilder).build();
			Resource.scheduler.rescheduleJob(triggerKey, trigger);
		}
		return true;
	}

	public boolean stopJob(String taskName) {
		JobKey jobKey = JobKey.jobKey(taskName, "DEFAULT");
		try {
			Resource.scheduler.pauseJob(jobKey);
		} catch (Exception e) {
			Common.LOG.error("stop task {} exception", taskName,e);
			return false;
		}
		return true;
	}

	public boolean startNow(String taskName) {
		JobKey jobKey = JobKey.jobKey(taskName, "DEFAULT");
		try {
			Resource.scheduler.triggerJob(jobKey);
		} catch (Exception e) {
			Common.LOG.error("start task {} exception",taskName, e);
			return false;
		}
		return true;
	}

	public boolean restartJob(String taskName) {
		JobKey jobKey = JobKey.jobKey(taskName, "DEFAULT");
		try {
			Resource.scheduler.resumeJob(jobKey);
		} catch (Exception e) {
			Common.LOG.error("restart task {} exception", taskName,e);
			return false;
		}
		return true;
	}

	public boolean deleteJob(String taskName) {
		JobKey taskKey = JobKey.jobKey(taskName, "DEFAULT");
		try {
			Resource.scheduler.deleteJob(taskKey);
		} catch (Exception e) {
			Common.LOG.error("delete task {} exception",taskName, e);
			return false;
		}
		return true;
	}
}
