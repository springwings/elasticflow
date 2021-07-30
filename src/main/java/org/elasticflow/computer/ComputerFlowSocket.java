/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.computer;

import java.util.LinkedList;
import java.util.Map;

import org.elasticflow.computer.handler.ComputeHandler;
import org.elasticflow.field.EFField;
import org.elasticflow.flow.Flow;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.computer.SamplePoint;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.util.DataSetReader;

public abstract class ComputerFlowSocket extends Flow { 
	
	/** defined custom Computer flow handler */
	protected ComputeHandler computerHandler;
	
	protected DataPage dataPage = new DataPage(); 
	
	protected LinkedList<PipeDataUnit> dataUnit = new LinkedList<>(); 
	
	@Override
	public void INIT(ConnectParams connectParams) {
		this.connectParams = connectParams; 
	}  
	
	public DataPage getDataPage() {
		return dataPage;
	}

	public LinkedList<PipeDataUnit> getDataUnit() {
		return dataUnit;
	}

	public ComputeHandler getComputerHandler() {
		return computerHandler;
	} 

	public void setComputerHandler(ComputeHandler computerHandler) {
		this.computerHandler = computerHandler;
	} 
	
	abstract public boolean loadModel(Object datas);
	
	abstract public DataPage train(Context context, DataSetReader DSR, Map<String, EFField> transParam);
	 
    /**
     * predicte the value of sample s
     * @param s : prediction sample
     * @return : predicted value
     */ 
	abstract public Object predict(SamplePoint point);
	
	abstract public DataPage predict(Context context,DataSetReader DSR);
	 
}
