package org.elasticflow.param.warehouse;

import java.util.HashMap;

import org.elasticflow.config.GlobalParam.DATA_TYPE;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-03-22 09:08
 */
public class WarehouseSqlParam implements WarehouseParam{
	
	private String name = "";
	private String alias;
	private String host = "";
	private int port;
	private String dbname = "";
	private String user = "";
	private String password = "";
	private DATA_TYPE type = DATA_TYPE.UNKNOWN;
	private String[] L1seq = {};
	private String handler;
	
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
	public String getDbname() {
		return dbname;
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
	public DATA_TYPE getType() {
		return this.type;
	}
	public void setType(String type) {
		if (type.equalsIgnoreCase("MYSQL"))
			this.type = DATA_TYPE.MYSQL;
		else if (type.equalsIgnoreCase("ORACLE"))
			this.type = DATA_TYPE.ORACLE;
		else if (type.equalsIgnoreCase("HIVE"))
			this.type = DATA_TYPE.HIVE;
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
	public HashMap<String, Object> getConnectParams(String seq) {
		HashMap<String, Object> connectParams = new HashMap<String, Object>();
		String dbname = (seq != null) ? getDbname().replace("#{seq}", seq) : getDbname();
		connectParams.put("host", getHost());
		connectParams.put("port", String.valueOf(getPort()));
		connectParams.put("dbname", dbname);
		connectParams.put("user", getUser());
		connectParams.put("password", getPassword()); 
		connectParams.put("type", getType());
		connectParams.put("poolName", getPoolName(seq));
		return connectParams;
	}	 
}
