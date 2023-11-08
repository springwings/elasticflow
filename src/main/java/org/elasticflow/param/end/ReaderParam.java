/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.param.end;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.FastDateFormat;
import org.elasticflow.util.Common;

import com.alibaba.fastjson.JSONObject;

/**
 * reader scan basic parameters
 * @author chengwen
 * @version 2.0
 * @date 2018-12-20 16:33
 */
public class ReaderParam {

	protected String keyField;
	/** value= int or string */
	protected String keyFieldType;
	protected String scanField;
	/**(data|time):(data配置y-m-d格式，time配置second|millisecond)*/
	protected String scanFieldType="time:second";
	protected String pageField;
	protected String pageScanDSL;
	private String dataScanDSL;
	private String handler;
	private boolean isNoSql = false;
	/** defined series scan location */
	protected List<String> L2seqs = Arrays.asList("");
	
	/**User defined JSON parameters can be used to extend the plugin*/
	private JSONObject customParams = new JSONObject();
	
	
	public JSONObject getCustomParams() {
		return customParams;
	}

	public void setCustomParams(String customParams) {
		if(customParams!=null) {
			this.customParams = JSONObject.parseObject(customParams);
		}	
	}
	
	public String getDataScanDSL() {
		return dataScanDSL;
	}
	public void setDataScanDSL(String dataScanDSL) {
		this.dataScanDSL = dataScanDSL.trim();
		if(this.dataScanDSL.length()>1 && this.dataScanDSL.substring(this.dataScanDSL.length()-1).equals(";")){
			this.dataScanDSL = this.dataScanDSL.substring(0,this.dataScanDSL.length()-1);
		}
	}  

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

	public void setScanFieldType(String scanFieldType) {
		this.scanFieldType = scanFieldType;
	}

	public String getPageField() {
		if(pageField==null)
			pageField = keyField;
		return pageField;
	}

	public void setPageField(String pageField) {
		this.pageField = pageField;
	}
	
	public String getPageScanDSL() {
		return pageScanDSL;
	} 

	public void setPageScanDSL(String pageScanDSL) {
		this.pageScanDSL = pageScanDSL;
	}

	public List<String> getL2Seq() {
		return L2seqs;
	}

	public void setSeq(String L2seqs) {
		this.L2seqs = Common.stringToList(L2seqs, ",");
	}

	public String getKeyFieldType() {
		return keyFieldType;
	}

	public void setKeyFieldType(String keyFieldType) {
		this.keyFieldType = keyFieldType;
	}
	
	public String getHandler() {
		return handler;
	}
	
	public void setHandler(String handler) {
		this.handler = handler;
	}  
	
	public boolean isNoSql() {
		return isNoSql;
	}
	public void setNoSql(boolean isNoSql) {
		this.isNoSql = isNoSql;
	}
	
	/**
	 * get current time
	 * @return
	 */
	public String getCurrentStamp() {
		if(scanFieldType.contains("date")) {
			FastDateFormat sdf = FastDateFormat.getInstance(scanFieldType.replace("data:", ""));   
			return sdf.format(new Date());
		}else {
			if(scanFieldType.contains("millisecond")) {
				return String.valueOf(System.currentTimeMillis());
			}
			return String.valueOf(System.currentTimeMillis()/1000);
		} 
	}
}
