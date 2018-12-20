package org.elasticflow.param.warehouse;

/**
 * 
 * @author chengwen
 * @version 4.0
 * @date 2018-10-26 09:11
 */
public class NoSQLParam extends ScanParam{ 
	private String mainTable;  
	
	public String getMainTable() {
		return mainTable;
	}
	public void setMainTable(String mainTable) {
		this.mainTable = mainTable;
	} 
 
	@Override
	public boolean isSqlType() { 
		return false;
	}  
}
