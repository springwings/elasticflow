package org.elasticflow.task.job;

import java.lang.reflect.Method;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.ETYPE;
import org.elasticflow.model.Localization;
import org.elasticflow.model.Localization.LAG_TYPE;
import org.elasticflow.model.task.TaskJobModel;
import org.elasticflow.util.Common;
import org.elasticflow.yarn.Resource;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

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
					Localization.format(LAG_TYPE.JobstartFailed, job.getInstanceID()),ETYPE.PARAMETER_ERROR.name(),true);
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
			Common.systemLog("task {} invoke method {} exception",jobModel.getJobName(),jobModel.getMethodName(), e);
		}
		return false;
	}
}