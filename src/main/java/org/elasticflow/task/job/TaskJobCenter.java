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

	public boolean addJob(TaskJobModel job) throws SchedulerException {
		if (job == null) {
			Common.LOG.error("task is null,add nothing!");
			return false;
		}

		TriggerKey triggerKey = TriggerKey.triggerKey(job.getJobName());
		CronTrigger trigger = (CronTrigger) Resource.scheduler.getTrigger(triggerKey);

		if (trigger == null) {
			Common.LOG.info("success add task {}", job.getJobName());
			JobDetail jobDetail = JobBuilder.newJob(JobRunFactory.class).withIdentity(job.getJobName()).build();
			jobDetail.getJobDataMap().put(GlobalParam.FLOW_TAG._DEFAULT.name(), job);
			CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(job.getCron());
			trigger = TriggerBuilder.newTrigger().withIdentity(job.getJobName(), job.getJobName())
					.withSchedule(scheduleBuilder).build();
			Resource.scheduler.scheduleJob(jobDetail, trigger);
		} else {
			Common.LOG.info("success modify task {}", job.getJobName());
			CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(job.getCron());
			trigger = trigger.getTriggerBuilder().withIdentity(triggerKey).withSchedule(scheduleBuilder).build();
			Resource.scheduler.rescheduleJob(triggerKey, trigger);
		}
		return true;
	}

	public boolean stopJob(String jobName) {
		JobKey jobKey = JobKey.jobKey(jobName, "DEFAULT");
		try {
			Resource.scheduler.pauseJob(jobKey);
		} catch (Exception e) {
			Common.LOG.error("Stop Task Exception", e);
			return false;
		}
		return true;
	}

	public boolean startNow(String jobName) {
		JobKey jobKey = JobKey.jobKey(jobName, "DEFAULT");
		try {
			Resource.scheduler.triggerJob(jobKey);
		} catch (Exception e) {
			Common.LOG.error("start task exception", e);
			return false;
		}
		return true;
	}

	public boolean restartJob(String jobName) {
		JobKey jobKey = JobKey.jobKey(jobName, "DEFAULT");
		try {
			Resource.scheduler.resumeJob(jobKey);
		} catch (Exception e) {
			Common.LOG.error("restart task exception", e);
			return false;
		}
		return true;
	}

	public boolean deleteJob(String jobName) {
		JobKey jobKey = JobKey.jobKey(jobName, "DEFAULT");
		try {
			Resource.scheduler.deleteJob(jobKey);
		} catch (Exception e) {
			Common.LOG.error("delete task exception", e);
			return false;
		}
		return true;
	}
}
