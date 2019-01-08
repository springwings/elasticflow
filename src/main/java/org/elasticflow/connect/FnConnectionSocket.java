package org.elasticflow.connect; 

import java.util.HashMap;

/** 
 * @author chengwen
 * @version 1.0
 * @param <T>
 * @date 2018-10-24 13:53
 */
public abstract class FnConnectionSocket<T>{
	
	protected volatile HashMap<String, Object> connectParams = null;
	
	private boolean isShare = false; 
	
	public abstract boolean connect(); 
	
	public abstract T getConnection(boolean searcher);
	
	public abstract boolean status();
	
	public abstract boolean free();
	
  
	public void init(HashMap<String, Object> ConnectParams) {
		this.connectParams = ConnectParams; 
	}
 
	public boolean isShare() { 
		return this.isShare;
	}
 
	public void setShare(boolean share) {
		this.isShare = share;
	}  
}
