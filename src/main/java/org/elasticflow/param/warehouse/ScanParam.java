package org.elasticflow.param.warehouse;

import java.util.ArrayList;
import java.util.List;

import org.elasticflow.util.Common;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public abstract class ScanParam {
	
	protected String keyField;
	/**value= int or string */
	protected String keyColumnType;
	protected String scanField = "";
	protected String pageScan;
	protected List<String> L2seqs = new ArrayList<String>();
	
	public abstract boolean isSqlType();
	 
	public String getKeyField() {
		return keyField;
	}
	public void setKeyField(String keyField) {
		this.keyField = keyField;
	}
	public String getScanField() {
		return scanField;
	}
	public void setScanField(String scanField) {
		this.scanField = scanField;
	} 
	
	public void setPageScan(String pageScan) {
		this.pageScan = pageScan;
	} 
	
	public List<String> getSeq() {
		return L2seqs;
	}
	public void setSeq(String L2seqs) {
		this.L2seqs = Common.stringToList(L2seqs, ",");
	}
}
