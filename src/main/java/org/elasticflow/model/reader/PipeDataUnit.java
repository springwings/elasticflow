/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.model.reader;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.field.EFField;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;

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
	
	public PipeDataUnit virtualWrite(Map<String, EFField> transParams) throws EFException {
		PipeDataUnit u = PipeDataUnit.getInstance();
		for (Entry<String, Object> r : data.entrySet()) {
			addFieldValue(r.getKey(), r.getValue(), transParams, u);
		}
		u.setReaderKeyVal(this.getReaderKeyVal());
		u.SYSTEM_UPDATE_TIME = this.SYSTEM_UPDATE_TIME;
		return u;		
	}
	
	public static void addFieldValue(String k,Object v,Map<String, EFField> transParams,PipeDataUnit pdu) throws EFException{
		EFField param = transParams.get(k);  
		if (param != null){
			Object val = parseFieldValue(v,param);
			if (param.getHandler()!=null) {
				param.getHandler().handle(pdu,param,val,transParams); 			  
			}else {
				pdu.data.put(param.getName(),val);
			}
		}else{ 
			if(transParams.size()==0)
				pdu.data.put(k,v);
		}
	}
	
	public static Object parseFieldValue(Object v, EFField fd) throws EFException {
		if (fd == null)
			return v; 
		if (v==null) {
			return fd.getDefaultvalue();
		}else {
			Class<?> c;
			try {
				if(fd.getParamtype().startsWith(GlobalParam.GROUPID) || fd.getParamtype().startsWith("java.lang")) {
					c = Class.forName(fd.getParamtype());	
				}else {
					c = Class.forName(fd.getParamtype(),true,GlobalParam.PLUGIN_CLASS_LOADER);
				}				
				if (fd.getSeparator() != null) {
					String[] vs = String.valueOf(v).split(fd.getSeparator());
					if(!fd.getParamtype().equals("java.lang.String")) {
						Object[] _vs = new Object[vs.length];
						Method method = c.getMethod("valueOf", String.class);
						for(int j=0;j<vs.length;j++) 
							_vs[j] = method.invoke(c,vs[j]); 
						return _vs;
					}
					return vs;
				 }else {				 
					 Method method = c.getMethod("valueOf", Object.class);
					 return method.invoke(c,v);
				 }
			} catch (Exception e) {
				throw new EFException(e.getMessage()+",Field "+fd.getName(), ELEVEL.Dispose);
			}			
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
