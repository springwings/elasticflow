package org.elasticflow.model.reader;

import java.util.HashMap;
import java.util.LinkedList;

import org.elasticflow.config.GlobalParam;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class DataPage extends HashMap<String, Object> {

	private static final long serialVersionUID = 8764060758588207664L;
	
	public DataPage() {
		this.put(GlobalParam.READER_STATUS,true);
	}
	
	public void putData(LinkedList<PipeDataUnit> data) {
		this.put("__DATAS", data);
	}
	
	public void putDataBoundary(String data) {
		this.put("__MAX_ID", data);
	}
	
	public String getDataBoundary() {
		return String.valueOf(this.get("__MAX_ID"));
	}
	
	@SuppressWarnings("unchecked")
	public LinkedList<PipeDataUnit> getData() {
		return (LinkedList<PipeDataUnit>) this.get("__DATAS");
	}  
	
	@Override
	public void clear() {
		super.clear();
		this.put(GlobalParam.READER_STATUS,true);
	}
	
	@Override
	public DataPage clone() { 
		DataPage dp  = (DataPage) super.clone();
		dp.put("__DATAS", getData().clone());
		return dp;
	}
}
