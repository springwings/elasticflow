package org.elasticflow.param.warehouse;

import java.util.List;

/**
 * 
 * @author chengwen
 * @version 4.0
 * @date 2018-10-26 09:11
 */
public class NoSQLParam implements ScanParam{ 
	private String mainTable; 
	private String keyColumn;
	private String incrementField = ""; 
	
	public String getMainTable() {
		return mainTable;
	}
	public void setMainTable(String mainTable) {
		this.mainTable = mainTable;
	} 
	public String getKeyColumn() {
		return keyColumn;
	}
	public void setKeyColumn(String keyColumn) {
		this.keyColumn = keyColumn;
	}
	public String getIncrementField() {
		return incrementField;
	}
	public void setIncrementField(String incrementField) {
		this.incrementField = incrementField;
	}
	@Override
	public boolean isSqlType() {
		// TODO Auto-generated method stub
		return false;
	} 
	@Override
	public List<String> getSeq() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setPageScan(String o) {
		// TODO Auto-generated method stub
		
	} 
}
