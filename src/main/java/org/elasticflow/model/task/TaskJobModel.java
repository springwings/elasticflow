package org.elasticflow.model.task;

/**
 * ScheduleJob model
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:21
 */
public class TaskJobModel {
	
	private String instanceID;
	private String jobName;
	private String cron;
	private String className;
	private String methodName;
	private Object object;

	public TaskJobModel(String instanceID,String jobName, String cronExpression, 
			String className, String methodName, Object object) {
		this.jobName = jobName;
		this.cron = cronExpression;
		this.className = className;
		this.methodName = methodName;
		this.object = object;
		this.instanceID = instanceID;
	}
	
	public String getInstanceID() {
		return instanceID;
	}

	public String getJobName() {
		return jobName;
	}

	public String getCron() {
		return cron;
	}

	public String getClassName() {
		return className;
	}

	public String getMethodName() {
		return methodName;
	}

	public Object getObject() {
		return object;
	}
}
