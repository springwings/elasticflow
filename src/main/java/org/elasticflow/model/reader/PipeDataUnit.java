/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.model.reader;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticflow.config.GlobalParam.FIELD_PARSE_TYPE;
import org.elasticflow.field.EFField;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;

/**
 * Build a virtual pipeline for data writing
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class PipeDataUnit implements Cloneable{  
	private String reader_key_val;
	private HashMap<String,Object> data;  
	private long SYSTEM_UPDATE_TIME; 
	
	public static PipeDataUnit getInstance(){
		return new PipeDataUnit();
	}
	public PipeDataUnit() {
		this.data = new HashMap<String,Object>();
		this.SYSTEM_UPDATE_TIME = System.currentTimeMillis();
	}
	
	/**
	 * Virtual pre-write to process data fields using the write side definition format
	 * @param transParams
	 * @return
	 * @throws EFException
	 */
	public PipeDataUnit virtualWrite(Map<String, EFField> transParams) throws EFException {
		PipeDataUnit u = PipeDataUnit.getInstance();
		for (Entry<String, EFField> r : transParams.entrySet()) {
			if(data.containsKey(r.getKey())) {
				addFieldValue(r.getKey(), data.get(r.getKey()), transParams, u);
			}else if(r.getValue().getDefaultvalue()!="") {
				u.data.put(r.getKey(),r.getValue().getDefaultvalue());
			}
		}
		u.setReaderKeyVal(this.getReaderKeyVal());
		u.SYSTEM_UPDATE_TIME = this.SYSTEM_UPDATE_TIME;
		return u;		
	}
	
	/**
	 * If handler exists, cross domain processing is first used by handler; 
	 * Finally, use paramtype process data
	 * @param k
	 * @param v
	 * @param transParams
	 * @param pdu
	 * @throws EFException
	 */
	public static void addFieldValue(String k,Object v,Map<String, EFField> transParams,PipeDataUnit pdu) throws EFException{
		EFField param = transParams.get(k);  
		if (param != null){			
			if (param.getHandler()!=null) {
				param.getHandler().handle(pdu,param,v,transParams); 	
				pdu.data.put(param.getName(),
						Common.parseFieldValue(pdu.data.get(param.getName()),param,FIELD_PARSE_TYPE.valueOf));
			}else {
				pdu.data.put(param.getName(),Common.parseFieldValue(v,param,FIELD_PARSE_TYPE.valueOf));
			}
		}else{ 
			if(transParams.size()==0)
				pdu.data.put(k,v);
		}
	}	
	
	public void setReaderKeyVal(Object reader_key_val){
		this.reader_key_val = String.valueOf(reader_key_val);
	}
 
	public HashMap<String,Object> getData() {
		return data;
	}
	
	public String getReaderKeyVal() {
		return reader_key_val;
	}
 
	public long getUpdateTime(){
		return this.SYSTEM_UPDATE_TIME;
	} 
	
	public Object clone() {  
		try {
			return super.clone();
		} catch (Exception e) {
			Common.LOG.error("Clone not support!"); 
		}
		return null;
	}
}
