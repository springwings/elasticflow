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
 * Configuration parameters model of SQL database
 * @author chengwen
 * @version 1.0
 * @date 2018-03-22 09:08
 */
public class WarehouseSqlParam implements WarehouseParam{
	
	/**It is instance name such as an table name**/
	private String name = "";
	/**Used to identify linked resources**/
	private String alias;
	private String host = "";
	private int port;
	private String dbname = "";
	private String user = "";
	private String password = "";
	private DATA_SOURCE_TYPE type = DATA_SOURCE_TYPE.UNKNOWN;
	private String[] L1seq = {};
	private String handler;
	private JSONObject customParams;
	private JSONObject defaultValue;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = Integer.valueOf(port);
	}
	@Override
	public String getHandler() {
		return handler;
	}
	public void setHandler(String handler) {
		this.handler = handler;
	}
	public String getDbname(String seq) {
		return (seq != null) ? this.dbname.replace("#{seq}", seq) : this.dbname; 
	}
	public void setDbname(String db) {
		this.dbname = db;
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
	public DATA_SOURCE_TYPE getType() {
		return this.type;
	}
	public void setType(String type) {
		if (type.equalsIgnoreCase("MYSQL"))
			this.type = DATA_SOURCE_TYPE.MYSQL;
		else if (type.equalsIgnoreCase("ORACLE"))
			this.type = DATA_SOURCE_TYPE.ORACLE;
		else if (type.equalsIgnoreCase("HIVE"))
			this.type = DATA_SOURCE_TYPE.HIVE;
		else if (type.equalsIgnoreCase("NEO4J"))
			this.type = DATA_SOURCE_TYPE.NEO4J;
	}
	
	public String getAlias() {
		if(this.alias==null){
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
		return this.alias+"_"+this.type+"_"+this.host+"_"+((seq != null) ? this.dbname.replace("#{seq}", seq):this.dbname);
	}
	@Override
	public int getMaxConn() { 
		return 0;
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
	
	public JSONObject getDefaultValue() {
		return this.defaultValue;
	}
	public void setDefaultValue(String defaultValue) {
		if(defaultValue!=null) {
			this.defaultValue = JSONObject.parseObject(defaultValue);
		}	
	} 
	  
}
