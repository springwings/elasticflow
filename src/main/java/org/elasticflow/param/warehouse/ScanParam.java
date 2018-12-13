package org.elasticflow.param.warehouse;

import java.util.List;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public interface ScanParam {
	
	public boolean isSqlType();
	
	public String getMainTable();

	public void setMainTable(String mainTable);

	public String getKeyColumn();

	public void setKeyColumn(String keyColumn);

	public String getIncrementField();

	public void setIncrementField(String incrementField);
	
	public void setPageScan(String o);
	
	public List<String> getSeq();
}
