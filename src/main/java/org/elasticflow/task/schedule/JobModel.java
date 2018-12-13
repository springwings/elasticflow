package org.elasticflow.task.schedule;

/**
 * ScheduleJob model
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:21
 */
public class JobModel {  
	   
    private String jobName;   
    private String cron; 
    private String className; 
    private String methodName; 
    private Object object;
    
    public JobModel(String jobName, String cronExpression, String className, String methodName, Object object){
    	this.jobName = jobName;
    	this.cron = cronExpression;
    	this.className = className;
    	this.methodName = methodName;
    	this.object = object;
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
