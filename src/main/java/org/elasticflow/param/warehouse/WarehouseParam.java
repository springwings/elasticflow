/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.param.warehouse;

import org.elasticflow.config.GlobalParam.DATA_SOURCE_TYPE;

import com.alibaba.fastjson.JSONObject;

/**
 * seq for series data position define
 * @author chengwen
 * @version 1.0 
 * @date 2018-07-22 09:08
 */
public class WarehouseParam {
	
	private DATA_SOURCE_TYPE type = DATA_SOURCE_TYPE.UNKNOWN;
	/**It is instance name such as an instance name**/
	private String name;
	/**Used to identify linked resources**/
	private String alias;
	/**Secondary resource identification ，Optional**/
	private String L1name = ""; 
	private String host = "";
	private String user = "";
	private String password = "";
	private int port;
	private JSONObject defaultValue;
	private JSONObject customParams;
	private String handler;
	private String[] L1seq = {};
	private int maxConn = 0;
	
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
		case "FILES":
			this.type = DATA_SOURCE_TYPE.FILES;
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
		case "FASTDFS":
			this.type = DATA_SOURCE_TYPE.FASTDFS;
			break;
		case "MYSQL":
			this.type = DATA_SOURCE_TYPE.MYSQL;
			break;
		case "ORACLE":
			this.type = DATA_SOURCE_TYPE.ORACLE;
			break;
		case "HIVE":
			this.type = DATA_SOURCE_TYPE.HIVE;
			break;
		case "NEO4J":
			this.type = DATA_SOURCE_TYPE.NEO4J;
			break;
		} 
	}
	public String getName(String seq) {
		return (seq != null) ? this.name.replace("#{seq}", seq) : this.name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getHost() {
		return host;
	}
	
	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	public int getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = Integer.valueOf(port);
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getHandler() {
		return handler;
	}
	public void setHandler(String handler) {
		this.handler = handler;
	}
	public JSONObject getDefaultValue() {
		return this.defaultValue;
	}
	public void setDefaultValue(String defaultValue) {
		if(defaultValue!=null) {
			this.defaultValue = JSONObject.parseObject(defaultValue);
		}	
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

	public String[] getL1seq() {
		return this.L1seq;
	}

	public void setL1seq(String seqs) {
		this.L1seq = seqs.split(",");
	}

	public String getL1name(String seq) {
		return (seq != null) ? this.L1name.replace("#{seq}", seq) : this.L1name;
	}

	public void setL1name(String db) {
		this.L1name = db;
	}
	
	public String getPoolName(String L1seq) { 
		String Lname;
		if(this.L1name!="") {
			Lname = (L1seq != null) ? this.L1name.replace("#{seq}", L1seq) : this.L1name;
		}else {
			Lname = L1seq;
		}
		return this.alias + "_" + this.type + "_" + this.host + "_" + Lname;
	}	

	public JSONObject getCustomParams() {
		return customParams;
	}

	public void setCustomParams(String customParams) {
		if(customParams!=null) {
			this.customParams = JSONObject.parseObject(customParams);
		}	
	}

	public int getMaxConn() {
		return this.maxConn;
	}

	public void setMaxConn(String maxConn) {
		if(maxConn!=null) {
			this.maxConn = Integer.parseInt(maxConn);
		}
	}	
}