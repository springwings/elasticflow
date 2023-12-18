/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.node;

import org.apache.commons.net.telnet.TelnetClient;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.yarn.Resource;

/**
 * Recover Master
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */

public class RecoverMonitor {
	 	
	private String takeIp;
	
	public void start() {
		Common.LOG.info("Start Recover Monitor Service!");
		String ip = GlobalParam.SystemConfig.getProperty("monitor_ip");
		while(true) {
			try {
				TelnetClient client = new TelnetClient(); 
				client.setDefaultTimeout(6000); 
				try { 
					client.connect(ip, 8617); 
				} catch (Exception e) {
					this.takeIp = ip;
					takeOverNode();
					return;
				}
				Thread.sleep(6000);
			}catch (Exception e) {
				Common.LOG.error("try to start Recover Monitor Service {} exception",ip,e);
			}  
		} 
	} 
	
	/**
	 * Take over the operation from the original node again
	 */
	private void returnNode() {
		TelnetClient client = new TelnetClient(); 
		client.setDefaultTimeout(2000); 
		while(true) {
			try {
				try { 
					client.connect(this.takeIp, 8617); 
					Common.LOG.info("restart and return Node {}.",this.takeIp);
					EFNodeUtil.runShell(GlobalParam.RESTART_SHELL_PATH);
					return;
				} catch (Exception e) { 
					Thread.sleep(5000); 
				} 
			}catch (Exception e) {
				Common.LOG.error("try to return Node {} exception",this.takeIp,e);
			}  
		}
	}
	
	private void takeOverNode() throws EFException { 
		Common.loadGlobalConfig(GlobalParam.DATAS_CONFIG_PATH+"/EF_NODES/"+this.takeIp+"/configs"); 
		Resource.EFLOWS.init(true);
		Resource.EFLOWS.startService();
		Common.LOG.info("{} has take Over Node {}",GlobalParam.IP,this.takeIp);
		new Thread() {
			public void run() {
				returnNode();
			}
		}.start();
	}  
}
