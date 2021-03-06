package org.elasticflow.task.schedule;

import java.lang.reflect.Method;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;
import org.elasticflow.yarn.Resource;

/**
 * job run factory Disallow Concurrent run the same job
 * 
 * @author chengwen
 * @version 1.1
 */
public class JobRunFactory implements Job {

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobModel job = (JobModel) context.getMergedJobDataMap().get(GlobalParam.FLOW_TAG._DEFAULT.name());
		if (!invokeMethod(job)) {
			Resource.mailSender.sendHtmlMailBySynchronizationMode(" ["+GlobalParam.PROJ+"] " + GlobalParam.RUN_ENV,
					"job [" + job.getJobName() + "] fire fail,with invokeMethod error!");
		}
	}
	
	/**
	 * if get static method jobModel.getObject() set null 
	 */
	private boolean invokeMethod(JobModel jobModel) {
		Object object = jobModel.getObject(); 
		try {
			Class<?> CL = object.getClass();
			Method method = CL.getDeclaredMethod(jobModel.getMethodName());
			method.invoke(object);
			return true;
		} catch (Exception e) {
			Common.LOG.error(jobModel.getJobName() + " invokMethod " + jobModel.getMethodName() + " Exception ", e);
		}
		return false;
	} 
}