package org.elasticflow.connect;

import java.util.HashMap;
 
/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public interface FnConnection<T>{ 
	
	public void init(HashMap<String, Object> ConnectParams);
	
	public boolean connect(); 
	
	public T getConnection(boolean searcher);
	
	public boolean status();
	
	public boolean free();
	
	public boolean isShare();
	
	public void setShare(boolean share);
}
