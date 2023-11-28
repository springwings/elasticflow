package org.elasticflow.task.job;

import java.lang.reflect.Method;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.model.Localization;
import org.elasticflow.model.Localization.LAG_TYPE;
import org.elasticflow.model.task.TaskJobModel;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
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
		TaskJobModel job = (TaskJobModel) context.getMergedJobDataMap().get(GlobalParam.FLOW_TAG._DEFAULT.name());
		if (!invokeMethod(job)) {			
			Resource.EfNotifier.send(Localization.format(LAG_TYPE.JobstartFailed, job.getInstanceID()),job.getInstanceID(),
					Localization.format(LAG_TYPE.JobstartFailed, job.getInstanceID()),EFException.ETYPE.PARAMETER_ERROR.name(),true);
		}
	}

	/**
	 * if get static method jobModel.getObject() set null
	 */
	private boolean invokeMethod(TaskJobModel jobModel) {
		Object object = jobModel.getObject();
		try {
			Class<?> CL = object.getClass();
			Method method = CL.getDeclaredMethod(jobModel.getMethodName());
			method.invoke(object);
			return true;
		} catch (Exception e) {
			Common.LOG.error(jobModel.getJobName() + " invoke method " + jobModel.getMethodName() + " Exception", e);
		}
		return false;
	}
}