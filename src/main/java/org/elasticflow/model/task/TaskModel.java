package org.elasticflow.model.task;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.param.end.ReaderParam;
import org.elasticflow.util.instance.TaskUtil;

/**
 * Task Definition and Description
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-23 14:36
 */
public final class TaskModel {
	
	private String instanceProcessId;
	private String instanceID;
	private String L1seq;
	private String L2seq;
	private InstanceConfig instanceConfig;
	/** sql source store with originalSql */
	private String additional;
	private JOB_TYPE jobType;
	public TaskState taskState;

	public static TaskModel getInstance(String instanceID, String L1seq, JOB_TYPE jobType, InstanceConfig instanceConfig,
			String additional) {
		TaskModel o = new TaskModel();
		o.instanceProcessId = TaskUtil.getInstanceProcessId(instanceID, L1seq);
		o.instanceID = instanceID;
		o.L1seq = L1seq;
		o.instanceConfig = instanceConfig;
		o.jobType = jobType;
		o.additional = additional;
		o.taskState = new TaskState();
		return o;
	}
	
	
	public InstanceConfig getInstanceConfig() {
		return this.instanceConfig;
	}

	public String getInstanceID() {
		return this.instanceID;
	}
	
	/**
	 * instance_L1seq
	 * @return
	 */
	public String getInstanceProcessId() {
		return this.instanceProcessId;
	}
	/**
	 * instance_L1seq_L2seq
	 * @return
	 */
	public String getInstanceProcessId(String L2seq) {
		return this.instanceProcessId+"_"+L2seq;
	}

	public String getL1seq() {
		return this.L1seq;
	}

	public String getL2seq() {
		return this.L2seq;
	}

	public void setL2seq(String L2seq) {
		this.L2seq = L2seq;
	}

	public ReaderParam getScanParam() {
		return this.instanceConfig.getReaderParams();
	}

	public String getStartTime() {
		return jobType.equals(JOB_TYPE.FULL) ? GlobalParam.TASK_COORDER.getScanPositon(instanceID,L1seq,L2seq,true)
				: GlobalParam.TASK_COORDER.getScanPositon(instanceID,L1seq,L2seq,false);
	}

	public String getEndTime() {
		return this.instanceConfig.getReaderParams().getCurrentStamp();
	}

	public JOB_TYPE getJobType() {
		return this.jobType;
	}
	
	public boolean isfull() {
		if(this.jobType==JOB_TYPE.FULL)
			return true;
		return false;
	}

	public String getAdditional() {
		return this.additional;
	}

}
