package org.elasticflow.param.warehouse;

import java.util.ArrayList;
import java.util.List;

import org.elasticflow.util.Common;

/**
 * 
 * @author chengwen
 * @version 4.0
 * @date 2018-10-25 16:18
 */
public class SQLParam implements ScanParam{
	private String sql;
	private String mainTable = "";
	private String mainAlias = "";
	private String keyColumn;
	/**value= int or string */
	private String keyColumnType;
	private String incrementField = "";
	private String pageScan;
	private String handler;
	private List<String> L2seqs = new ArrayList<String>();
	
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql.trim();
		if(this.sql.length()>1 && this.sql.substring(this.sql.length()-1).equals(";")){
			this.sql = this.sql.substring(0,this.sql.length()-1);
		}
	}
	
	public String getMainTable() {
		return mainTable;
	}
	public void setMainTable(String mainTable) {
		this.mainTable = mainTable;
	}
	public String getMainAlias() {
		return mainAlias;
	}
	public void setMainAlias(String mainAlias) {
		this.mainAlias = mainAlias;
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
	public List<String> getSeq() {
		return L2seqs;
	}
	public void setSeq(String L2seqs) {
		this.L2seqs = Common.stringToList(L2seqs, ",");
	}
	public String getPageScan() {
		return pageScan;
	} 
	@Override
	public void setPageScan(String pageScan) {
		this.pageScan = pageScan;
	} 
	public String getKeyColumnType() {
		return keyColumnType;
	}
	public void setKeyColumnType(String keyColumnType) {
		this.keyColumnType = keyColumnType;
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
