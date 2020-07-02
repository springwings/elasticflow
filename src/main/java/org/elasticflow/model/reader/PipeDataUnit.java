package org.elasticflow.model.reader;

import java.util.HashMap;
import java.util.Map;

import org.elasticflow.field.EFField;
import org.elasticflow.util.Common;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class PipeDataUnit implements Cloneable{  
	public String reader_key_val;
	private HashMap<String,Object> data;  
	private long SYSTEM_UPDATE_TIME; 
	
	public static PipeDataUnit getInstance(){
		return new PipeDataUnit();
	}
	public PipeDataUnit() {
		this.data = new HashMap<String,Object>();
		this.SYSTEM_UPDATE_TIME = System.currentTimeMillis();
	}
	
	public boolean addFieldValue(String k,Object v,Map<String, EFField> transParams){
		EFField param = transParams.get(k);
		if (param != null){
			if (param.getHandler()!=null){
				param.getHandler().handle(this, v,transParams); 
			}
			else{ 
				this.data.put(param.getAlias(),v);
			}
			return true;
		}else{
			return false;
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
