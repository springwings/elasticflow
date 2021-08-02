/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.param.warehouse;

import org.elasticflow.config.GlobalParam.DATA_SOURCE_TYPE;

import com.alibaba.fastjson.JSONObject;;

/**
 * Configuration parameters model of NoSQL database
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class WarehouseNosqlParam implements WarehouseParam{
	
	private DATA_SOURCE_TYPE type = DATA_SOURCE_TYPE.UNKNOWN;
	/**It is instance name such as an index name**/
	private String name;
	/**Used to identify linked resources**/
	private String alias;
	private String path;
	private String defaultValue;
	private JSONObject customParams;
	private String handler;
	private String[] L1seq = {};
	
	public DATA_SOURCE_TYPE getType() {
		return type;
	}
	
	public void setType(String type) {
		switch (type.toUpperCase()) {
		case "SOLR":
			this.type = DATA_SOURCE_TYPE.SOLR;
			break;
		case "ES":
			this.type = DATA_SOURCE_TYPE.ES;
			break;
		case "HBASE":
			this.type = DATA_SOURCE_TYPE.HBASE;
			break;
		case "FILE":
			this.type = DATA_SOURCE_TYPE.FILE;
			break;
		case "KAFKA":
			this.type = DATA_SOURCE_TYPE.KAFKA;
			break;
		case "VEARCH":
			this.type = DATA_SOURCE_TYPE.VEARCH;
			break;
		case "HDFS":
			this.type = DATA_SOURCE_TYPE.HDFS;
			break;
		} 
	}
	public String getName(String seq) {
		return (seq != null) ? this.name.replace("#{seq}", seq) : this.name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	@Override
	public String getHandler() {
		return handler;
	}
	public void setHandler(String handler) {
		this.handler = handler;
	}
	public String getDefaultValue() {
		return this.defaultValue;
	}
	public void setDefaultValue(String defaultvalue) {
		this.defaultValue = defaultvalue;
	} 
	public String getAlias() {
		if(this.alias == null){
			this.alias = this.name;
		}
		return this.alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	@Override
	public String[] getL1seq() {
		return this.L1seq;
	}
	@Override
	public void setL1seq(String seqs) {
		this.L1seq = seqs.split(",");
	}

	@Override
	public String getPoolName(String seq) { 
		return ((seq != null) ? this.alias.replace("#{seq}", seq):this.alias)+"_"+this.type+"_"+this.path;
	}	
	
	@Override
	public JSONObject getCustomParams() {
		return customParams;
	}

	public void setCustomParams(String customParams) {
		if(customParams!=null) {
			this.customParams = JSONObject.parseObject(customParams);
		}	
	}

	@Override
	public int getMaxConn() {
		// TODO Auto-generated method stub
		return 0;
	} 
 
}
