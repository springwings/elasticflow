package org.elasticflow.model;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.param.warehouse.ScanParam;
import org.elasticflow.reader.handler.Handler;
import org.elasticflow.util.Common;

public final class Task {
	private String instance;
	private String L1seq;
	private String L2seq;
	private ScanParam scanParam;
	/** sql source store with originalSql */ 
	private String additional;
	private JOB_TYPE jobType;

	public static Task getInstance(String instance, String L1seq,JOB_TYPE jobType,ScanParam scanParam, 
			String additional,Handler readHandler) {
		Task o = new Task();
		o.instance = instance;
		o.L1seq = L1seq;
		o.scanParam = scanParam; 
		o.jobType = jobType;
		o.additional = additional;
		if (readHandler != null)
			readHandler.handlePage("", o);
		return o;
	}

	public String getInstance() {
		return instance;
	}

	public String getL1seq() {
		return L1seq;
	}
	
	public String getL2seq() {
		return L2seq;
	}

	public void setL2seq(String L2seq) {
		this.L2seq = L2seq;
	}

	public ScanParam getScanParam() {
		return scanParam;
	}

	public String getStartTime() {
		return jobType.equals(JOB_TYPE.FULL)?Common.getFullStartInfo(instance, L1seq)
				: GlobalParam.SCAN_POSITION.get(Common.getMainName(instance, L1seq)).getL2SeqPos(L2seq);
	}

	public String getEndTime() {
		return scanParam.getCurrentStamp();
	}

	public JOB_TYPE getJobType() {
		return jobType;
	}
 
	public String getAdditional() {
		return additional;
	}

}
