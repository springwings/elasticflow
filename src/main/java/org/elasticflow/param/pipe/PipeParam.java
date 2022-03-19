/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.param.pipe;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.MECHANISM;

/**
 * The control parameters of data-flow pipeline model
 * @author chengwen
 * @version 4.0
 * @date 2018-10-25 16:14
 */
public class PipeParam { 
	private int readPageSize = GlobalParam.READ_PAGE_SIZE;
	private int logLevel = 0;
	private int failFreq = 5;
	private int maxFailTime = 100;
	private String writeTo;
	private String customWriter;
	private boolean writerPoolShareAlias = false;
	private String searchFrom;
	private String customSearcher;
	private boolean searcherShareAlias = true;
	private String readFrom;
	private String customReader;
	private boolean readerPoolShareAlias = false;
	private String deltaCron;
	private String fullCron;
	private String optimizeCron; 
	/**specify reference instance ,task will only can start by master**/
	private String referenceInstance;
	private String[] nextJob = new String[]{};
	/**flow batch task processing use strict transaction control or not. **/
	private boolean transactionControl;
	/** default is real pipe,if is virtual will only manage pipe-end with no data flow! */
	private boolean virtualPipe = false;
	/** control each slave instance run in Concurrent mode or not **/
	private boolean async = false;
	/**data write into type,full create new record,increment update part of data*/
	private boolean writeType = false;
	private MECHANISM writeMechanism = MECHANISM.NORM;
	/**if MECHANISM.Time,keepNums is used for keep max store data instances*/
	private String keepNums = "30d";
	private boolean multiThread = false;
	/**Task priority control**/
	private int priority = 9;
	
	public void reInit() {
		if(referenceInstance!=null) {
			this.deltaCron = null;
			this.fullCron = null;
			this.virtualPipe = false;
			this.optimizeCron = null;
		}
	}
	
	public boolean isTransactionControl() {
		return transactionControl;
	}
	
	public void setTransactionControl(String transactionControl) {
		this.transactionControl = Boolean.valueOf(transactionControl);;
	}
	
	public String getWriteTo() {
		return writeTo;
	}
	
	public int getReadPageSize() {
		return readPageSize;
	}
	
	public int getPriority() {
		return priority;
	}
	
	public int getLogLevel() {
		return logLevel;
	}
	
	public int getFailFreq() {
		return failFreq;
	}
	
	public int getMaxFailTime() {
		return maxFailTime;
	}
	
	public boolean showInfoLog() {
		if(logLevel==0)
			return true;
		return false;
	}

	public MECHANISM getWriteMechanism() {
		return writeMechanism;
	}

	public String getReadFrom() {
		return readFrom;
	}
	public String[] getNextJob() {
		return nextJob;
	}
	public boolean isMultiThread() {
		return multiThread;
	} 
	public String getCustomReader() { 
		return customReader;
	}
	public String getCustomWriter() {
		return customWriter;
	}
	public String getCustomSearcher() {
		return customSearcher;
	} 
	public String getReferenceInstance() {
		return referenceInstance;
	} 
	public String getDeltaCron() {
		return deltaCron;
	}
	public void setDeltaCron(String deltaCron) {
		this.deltaCron = deltaCron;
	}
	public String getFullCron() {
		return fullCron;
	}
	public void setFullCron(String fullCron) {
		this.fullCron = fullCron;
	} 
	public void setKeepNums(String keepNums) {
		this.keepNums = keepNums;
	} 
	public String getOptimizeCron() {
		return optimizeCron;
	}
	public void setOptimizeCron(String optimizeCron) {
		this.optimizeCron = optimizeCron;
	}  
	
	public void setLogLevel(String logLevel) {
		this.logLevel = Integer.valueOf(logLevel);
	}
	
	public void setFailFreq(String failFreq) {
		this.failFreq = Integer.valueOf(failFreq);
	}
	
	public void setMaxFailTime(String maxFailTime) {
		this.maxFailTime = Integer.valueOf(maxFailTime);
	}
	
	public void setPriority(String priority) {
		this.priority = Integer.valueOf(priority);
	}
	
	public void setWriteTo(String writeTo) {
		this.writeTo = writeTo;
	}
	
	/**
	 * get keep type and data
	 * @return int[],0 store type (0 day 1 month),1 store data
	 */
	public int[] getKeepNums() {
		int[] dt = new int[2];
		dt[1] = Integer.parseInt(this.keepNums.toLowerCase().substring(0, this.keepNums.length()-1));
		if(this.keepNums.toLowerCase().endsWith("d")) {			
			dt[0] = 0;
		}else {
			dt[0] = 1;
		}
		return dt;
	}
	
	public String getSearchFrom() {
		if(this.searchFrom==null){
			this.searchFrom = this.writeTo;
		}
		return this.searchFrom;
	} 
	
	public boolean isUpdateWriteType() {
		return writeType;
	} 
 
	public boolean isWriterPoolShareAlias() {
		return writerPoolShareAlias;
	} 
	
	public boolean isReaderPoolShareAlias() {
		return readerPoolShareAlias;
	} 

	public boolean isSearcherShareAlias() {
		return searcherShareAlias;
	}

	public boolean isVirtualPipe() {
		return virtualPipe;
	}  
	
	public boolean isAsync() {
		return async;
	}
	
	public void setReadPageSize(String readPageSize) {
		this.readPageSize = Integer.valueOf(readPageSize);
	}

	public void setCustomWriter(String customWriter) {
		this.customWriter = customWriter;
	}
	public void setWriterPoolShareAlias(String writerPoolShareAlias) {
		this.writerPoolShareAlias = Boolean.valueOf(writerPoolShareAlias);
	}
	public void setSearchFrom(String searchFrom) {
		this.searchFrom = searchFrom;
	}
	public void setCustomSearcher(String customSearcher) {
		this.customSearcher = customSearcher;
	}
	public void setSearcherShareAlias(String searcherShareAlias) {
		this.searcherShareAlias = Boolean.valueOf(searcherShareAlias);
	}
	public void setReadFrom(String readFrom) {
		this.readFrom = readFrom;
	}
	public void setCustomReader(String customReader) {
		this.customReader = customReader;
	}
	public void setReaderPoolShareAlias(String readerPoolShareAlias) { 
		this.readerPoolShareAlias = Boolean.valueOf(readerPoolShareAlias);
	}
	public void setReferenceInstance(String referenceInstance) {
		this.referenceInstance = referenceInstance;
	}
	public void setNextJob(String nextJob) {
		this.nextJob = nextJob.replace(",", " ").trim().split(" ");
	}
	public void setVirtualPipe(String virtualPipe) {
		if(virtualPipe.length()>0 && virtualPipe.toLowerCase().equals("true"))
			this.virtualPipe = true;
	}
	
	public void setAsync(String async) {
		if(async.length()>0 && async.toLowerCase().equals("true"))
			this.async = true;
	}
	
	public void setWriteType(String writeType) {
		if(writeType.length()>0 && writeType.equals("increment"))
			this.writeType = true;
		
	}
	public void setWriteMechanism(String writeMechanism) {
		switch(writeMechanism.toLowerCase()) {
			case "ab":
				this.writeMechanism = MECHANISM.AB;
				break;
			case "time":
				this.writeMechanism = MECHANISM.Time;
				break;				
		}		 
	}
	public void setMultiThread(String multiThread) {
		if(multiThread.length()>0 && (multiThread.equals("true")))
			this.multiThread = true; 
	} 
}
