package org.elasticflow.model;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.param.end.ReaderParam;
import org.elasticflow.util.Common;

/**
 * Task
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-23 14:36
 */
public final class Task {

	private String instance;
	private String L1seq;
	private String L2seq;
	private InstanceConfig instanceConfig;
	/** sql source store with originalSql */
	private String additional;
	private JOB_TYPE jobType;
	public TaskState taskState;

	public static Task getInstance(String instance, String L1seq, JOB_TYPE jobType, InstanceConfig instanceConfig,
			String additional) {
		Task o = new Task();
		o.instance = instance;
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

	public String getInstance() {
		return this.instance;
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
		return this.instanceConfig.getReadParams();
	}

	public String getStartTime() {
		return jobType.equals(JOB_TYPE.FULL) ? Common.getFullStartInfo(instance, L1seq)
				: GlobalParam.SCAN_POSITION.get(Common.getMainName(instance, L1seq)).getL2SeqPos(L2seq);
	}

	public String getEndTime() {
		return this.instanceConfig.getReadParams().getCurrentStamp();
	}

	public JOB_TYPE getJobType() {
		return this.jobType;
	}

	public String getAdditional() {
		return this.additional;
	}

}
