package org.elasticflow.task.schedule;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.springframework.beans.factory.annotation.Autowired;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;

/**
 * shedule job manager
 * for add,remove,stop,restart,startonce manager
 * @author chengwen
 * @version 1.0 
 */
public class TaskJobCenter{
	
	@Autowired
	Scheduler scheduler;

	public boolean addJob(JobModel job) throws SchedulerException {
		if (job == null){
			Common.LOG.error("Job is null nothing to add!");
			return false;
		} 

		TriggerKey triggerKey = TriggerKey.triggerKey(job.getJobName());
		CronTrigger trigger = (CronTrigger) scheduler.getTrigger(triggerKey);

		if (trigger == null) {
			Common.LOG.info("Add Schedule Job " + job.getJobName());			
			JobDetail jobDetail = JobBuilder.newJob(JobRunFactory.class).withIdentity(job.getJobName()).build();
			jobDetail.getJobDataMap().put(GlobalParam.FLOW_TAG._DEFAULT.name(), job); 
			CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(job.getCron());
			trigger = TriggerBuilder.newTrigger().withIdentity(job.getJobName(),job.getJobName()).withSchedule(scheduleBuilder).build();
			scheduler.scheduleJob(jobDetail, trigger);
		} else {
			Common.LOG.info("Modify Schedule Job " + job.getJobName());
			CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(job.getCron());
			trigger = trigger.getTriggerBuilder().withIdentity(triggerKey).withSchedule(scheduleBuilder).build();
			scheduler.rescheduleJob(triggerKey, trigger);
		}
		return true;
	}
	
	public boolean stopJob(String jobName){
		JobKey jobKey = JobKey.jobKey(jobName,"DEFAULT");
		try { 
			scheduler.pauseJob(jobKey);
		} catch (Exception e) {
			Common.LOG.error("Stop Job Exception",e);
			return false;
		} 	 
		return true;
	}
	
	public boolean startNow(String jobName){
		JobKey jobKey = JobKey.jobKey(jobName,"DEFAULT");
		try {
			scheduler.triggerJob(jobKey);
		} catch (Exception e) {
			Common.LOG.error("SchedulerException start do Job now",e);
			return false;
		}
		return true;
	}
	
	public boolean restartJob(String jobName){
		JobKey jobKey = JobKey.jobKey(jobName,"DEFAULT");
		try {
			scheduler.resumeJob(jobKey);
		} catch (Exception e) {
			Common.LOG.error("SchedulerException restart Job",e);
			return false;
		}
		return true;
	}

	public boolean deleteJob(String jobName) {   
		JobKey jobKey = JobKey.jobKey(jobName,"DEFAULT");
		try {
			scheduler.deleteJob(jobKey);
		} catch (Exception e) {
			Common.LOG.error("SchedulerException delete Job",e);
			return false;
		} 
		return true;
    }  
}
