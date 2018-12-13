package org.elasticflow.param.end;

import java.util.List;

import org.elasticflow.param.warehouse.SQLParam;
import org.elasticflow.param.warehouse.ScanParam;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-02-22 09:08
 */
public class MessageParam implements ScanParam{
	private SQLParam sqlParam; 
	private String se;
	private String type;
	private String topic;
	private String handler;
	public SQLParam getSqlParam() {
		return sqlParam;
	}
	public void setSqlParam(SQLParam sqlParam) {
		this.sqlParam = sqlParam;
	}
	public String getSe() {
		return se;
	}
	public void setSe(String se) {
		this.se = se;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getHandler() {
		return handler;
	}
	public void setHandler(String handler) {
		this.handler = handler;
	}
	@Override
	public boolean isSqlType() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public String getMainTable() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setMainTable(String mainTable) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public String getKeyColumn() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setKeyColumn(String keyColumn) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public String getIncrementField() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setIncrementField(String incrementField) {
		// TODO Auto-generated method stub
		
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
