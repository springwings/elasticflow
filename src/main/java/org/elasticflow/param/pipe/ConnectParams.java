package org.elasticflow.param.pipe;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.param.warehouse.WarehouseParam;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
public class ConnectParams {
	private String L1Seq; 
	private Object plugin;
	private volatile WarehouseParam whp;
	private volatile InstanceConfig instanceConfig; 
	
	public static ConnectParams getInstance(WarehouseParam whp,String L1Seq,InstanceConfig instanceConfig,Object plugin) {
		ConnectParams o = new ConnectParams();
		o.whp = whp;
		o.L1Seq = L1Seq;
		o.instanceConfig = instanceConfig;
		o.plugin = plugin;
		return o; 
	}
	
	public String getL1Seq() {
		return L1Seq;
	}

	public void setL1Seq(String l1Seq) {
		L1Seq = l1Seq;
	}

	public WarehouseParam getWhp() {
		return whp;
	}

	public void setWhp(WarehouseParam whp) {
		this.whp = whp;
	}

	public InstanceConfig getInstanceConfig() {
		return instanceConfig;
	}

	public void setInstanceConfig(InstanceConfig instanceConfig) {
		this.instanceConfig = instanceConfig;
	}

	public Object getPlugin() {
		return plugin;
	}

	public void setPlugin(Object plugin) {
		this.plugin = plugin;
	} 
}
