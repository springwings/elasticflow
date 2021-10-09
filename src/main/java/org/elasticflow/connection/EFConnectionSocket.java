package org.elasticflow.connection; 

import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;

/** 
 * @author chengwen
 * @version 1.0
 * @param <T>
 * @date 2018-10-24 13:53
 */
public abstract class EFConnectionSocket<T>{
	
	protected volatile ConnectParams connectParams;
	
	private boolean isShare = false; 
	
	public abstract boolean connect(); 
	
	public abstract T getConnection(END_TYPE endtype);
	
	public abstract boolean status();
	
	public abstract boolean free();
	  
	public void init(ConnectParams ConnectParams) {
		this.connectParams = ConnectParams; 
	}
 
	public boolean isShare() { 
		return this.isShare;
	}
 
	public void setShare(boolean share) {
		this.isShare = share;
	}  
	
	public ConnectParams getConnectParams(){
		return this.connectParams;
	}
}
