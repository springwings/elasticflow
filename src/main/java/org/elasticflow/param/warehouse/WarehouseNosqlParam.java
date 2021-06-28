package org.elasticflow.param.warehouse;

import org.elasticflow.config.GlobalParam.DATA_SOURCE_TYPE;;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class WarehouseNosqlParam implements WarehouseParam{
	
	private DATA_SOURCE_TYPE type = DATA_SOURCE_TYPE.UNKNOWN;
	private String name;
	private String alias;
	private String path;
	private String defaultValue;
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
	public int getMaxConn() {
		// TODO Auto-generated method stub
		return 0;
	} 
 
}
