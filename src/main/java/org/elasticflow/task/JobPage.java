package org.elasticflow.task;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.field.RiverField;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-08 16:49
 */
public class JobPage {
	private long id;
	private String instance;
	public CountDownLatch leftPage;
	private JOB_TYPE job_type;
	private String L2seq;
	private String sql;
	private String incrementField;
	private String keyColumn;
	private Map<String, RiverField> transField;
	private long timeStamp = System.currentTimeMillis();
 
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getInstance() {
		return instance;
	}

	public void setInstance(String instance) {
		this.instance = instance;
	}

	public JOB_TYPE getJob_type() {
		return job_type;
	}

	public void setJob_type(JOB_TYPE job_type) {
		this.job_type = job_type;
	}

	public String getL2seq() {
		return L2seq;
	}

	public void setL2seq(String L2seq) {
		this.L2seq = L2seq;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getIncrementField() {
		return incrementField;
	}

	public void setIncrementField(String incrementField) {
		this.incrementField = incrementField;
	}

	public String getKeyColumn() {
		return keyColumn;
	}

	public void setKeyColumn(String keyColumn) {
		this.keyColumn = keyColumn;
	}

	public Map<String, RiverField> getTransField() {
		return transField;
	}

	public void setTransField(Map<String, RiverField> transField) {
		this.transField = transField;
	}

	public long getTimeStamp() {
		return timeStamp;
	}
}
