/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.computer;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.elasticflow.computer.handler.ComputerHandler;
import org.elasticflow.field.EFField;
import org.elasticflow.flow.Flow;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.computer.SamplePoint;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.EFException;

/**
 * Computer Flow Socket
 * @author chengwen
 * @version 1.0
 * @date 2018-12-28 09:27
 */
public abstract class ComputerFlowSocket extends Flow{ 
	
	/** defined custom Computer flow handler */
	protected ComputerHandler computerHandler;
	
	protected DataPage dataPage = new DataPage(); 
	
	protected ConcurrentLinkedQueue<PipeDataUnit> dataUnit = new ConcurrentLinkedQueue<>(); 
	
	@Override
	public void initConn(ConnectParams connectParams) {
		this.connectParams = connectParams; 
	}  
	
	@Override
	public void initFlow() {
		//auto invoke in flow prepare
	}
	
	public DataPage getDataPage() {
		return dataPage;
	}

	public ConcurrentLinkedQueue<PipeDataUnit> getDataUnit() {
		return dataUnit;
	}

	public ComputerHandler getComputerHandler() {
		return computerHandler;
	} 

	public void setComputerHandler(ComputerHandler computerHandler) {
		this.computerHandler = computerHandler;
	} 
	
	abstract public boolean loadModel(Object datas);
	
	abstract public DataPage train(Context context, DataSetReader DSR, Map<String, EFField> transParam) throws EFException;
	 
    /**
     * predicte the value of sample s
     * @param s : prediction sample
     * @return : predicted value
     */ 
	abstract public Object predict(SamplePoint point) throws EFException;
	
	abstract public DataPage predict(Context context,DataSetReader DSR) throws EFException;
	 
}
