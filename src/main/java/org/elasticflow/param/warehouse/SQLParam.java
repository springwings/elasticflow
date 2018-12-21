package org.elasticflow.param.warehouse;

/**
 * 
 * @author chengwen
 * @version 4.0
 * @date 2018-10-25 16:18
 */
public class SQLParam extends ScanParam{
	private String sql; 
	private String handler; 
	
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql.trim();
		if(this.sql.length()>1 && this.sql.substring(this.sql.length()-1).equals(";")){
			this.sql = this.sql.substring(0,this.sql.length()-1);
		}
	}  
	
	public String getPageScan() {
		return pageScan;
	} 
	@Override
	public void setPageScan(String pageScan) {
		this.pageScan = pageScan;
	} 

	public String getHandler() {
		return handler;
	}
	public void setHandler(String handler) {
		this.handler = handler;
	}  
	@Override
	public boolean isSqlType() { 
		return true;
	} 
}
