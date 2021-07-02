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
 * data-flow trans parameters
 * @author chengwen
 * @version 4.0
 * @date 2018-10-25 16:14
 */
public class PipeParam { 
	private int readPageSize = GlobalParam.READ_PAGE_SIZE;
	private int logLevel = 0;
	private String writeTo;
	private String modelFrom;
	private String writeHandler;
	private boolean writerPoolShareAlias = true;
	private String searchFrom;
	private String searcherHandler;
	private boolean searcherShareAlias = true;
	private String readFrom;
	private String readHandler;
	private boolean readerPoolShareAlias = false;
	private String deltaCron;
	private String fullCron;
	private String optimizeCron; 
	private String instanceName;
	private String[] nextJob;
	/** default is slave pipe,if is master will only manage pipe with no detail transfer job! */
	private boolean isMaster = false;
	/** control each slave instance run in Concurrent mode or not **/
	private boolean async = false;
	/** slave node deep control by master **/
	private boolean isMasterControl = false;
	/**data write into type,full create new record,increment update part of data*/
	private String writeType="full";
	private MECHANISM writeMechanism = MECHANISM.AB;
	/**if MECHANISM.Time,keepNums is used for keep max store data instances*/
	private String keepNums = "30d";
	private boolean multiThread = false;
	
	 
	public String getWriteTo() {
		return writeTo;
	}
	public int getReadPageSize() {
		return readPageSize;
	}
	public int getLogLevel() {
		return logLevel;
	}
	public String getModelFrom() {
		return modelFrom;
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
	public String getReadHandler() { 
		return readHandler;
	}
	public String getWriteHandler() {
		return writeHandler;
	}
	public String getSearcherHandler() {
		return searcherHandler;
	} 
	public String getInstanceName() {
		return instanceName;
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
	
	public String getWriteType() {
		return writeType;
	} 
	
	public void setInstancename(String v) {
		this.instanceName = v;
	}
	
	public boolean isWriterPoolShareAlias() {
		return writerPoolShareAlias;
	} 
	
	public boolean isReaderPoolShareAlias() {
		return readerPoolShareAlias;
	} 
	
	public boolean isMasterControl() {
		return isMasterControl;
	}

	public boolean isSearcherShareAlias() {
		return searcherShareAlias;
	}

	public boolean isMaster() {
		return isMaster;
	}  
	
	public boolean isAsync() {
		return async;
	}
	
	public void setReadPageSize(int readPageSize) {
		this.readPageSize = Integer.valueOf(readPageSize);
	}
	public void setModelFrom(String modelFrom) {
		this.modelFrom = modelFrom;
	}
	public void setWriteHandler(String writeHandler) {
		this.writeHandler = writeHandler;
	}
	public void setWriterPoolShareAlias(String writerPoolShareAlias) {
		this.writerPoolShareAlias = Boolean.valueOf(writerPoolShareAlias);
	}
	public void setSearchFrom(String searchFrom) {
		this.searchFrom = searchFrom;
	}
	public void setSearcherHandler(String searcherHandler) {
		this.searcherHandler = searcherHandler;
	}
	public void setSearcherShareAlias(String searcherShareAlias) {
		this.searcherShareAlias = Boolean.valueOf(searcherShareAlias);
	}
	public void setReadFrom(String readFrom) {
		this.readFrom = readFrom;
	}
	public void setReadHandler(String readHandler) {
		this.readHandler = readHandler;
	}
	public void setReaderPoolShareAlias(String readerPoolShareAlias) { 
		this.readerPoolShareAlias = Boolean.valueOf(readerPoolShareAlias);
	}
	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}
	public void setNextJob(String nextJob) {
		this.nextJob = nextJob.replace(",", " ").trim().split(" ");
	}
	public void setIsMaster(String isMaster) {
		if(isMaster.length()>0 && isMaster.toLowerCase().equals("true"))
			this.isMaster = true;
	}
	
	public void setAsync(String async) {
		if(async.length()>0 && async.toLowerCase().equals("true"))
			this.async = true;
	}
	
	public void setIsMasterControl(String isMasterControl) { 
		if(isMasterControl.length()>0 && isMasterControl.toLowerCase().equals("true"))
			this.isMasterControl = true;
	}
	
	public void setWriteType(String writeType) {
		if(writeType.length()>0 && (writeType.equals("full") || writeType.equals("increment")))
			this.writeType = writeType;
		
	}
	public void setWriteMechanism(String writeMechanism) {
		if(!writeMechanism.toLowerCase().equals("ab")) {
			this.writeMechanism = MECHANISM.Time;
		} 
	}
	public void setMultiThread(String multiThread) {
		if(multiThread.length()>0 && (multiThread.equals("true")))
			this.multiThread = true; 
	} 
}
