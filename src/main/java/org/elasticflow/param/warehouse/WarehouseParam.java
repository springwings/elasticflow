package org.elasticflow.param.warehouse;

import java.util.HashMap;

import org.elasticflow.config.GlobalParam.DATA_TYPE;

/**
 * seq for series data position define
 * @author chengwen
 * @version 1.0 
 */
public interface WarehouseParam {
	
	public String[] getL1seq();
	
	public void setL1seq(String seqs);
	
	public DATA_TYPE getType();
	
	public String getHandler(); 
	
	public String getPoolName(String seq);
	
	public HashMap<String, Object> getConnectParams(String seq);	
}
