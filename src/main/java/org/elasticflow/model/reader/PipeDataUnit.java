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
	public String key_column_val;
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
	
	public void setKeyColumnVal(Object key_column_val){
		this.key_column_val = String.valueOf(key_column_val);
	}
 
	public HashMap<String,Object> getData() {
		return data;
	}
	
	public String getKeyColumnVal() {
		return key_column_val;
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
