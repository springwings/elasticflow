package org.elasticflow.node;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFNodeUtil;

/**
 * Safe exit system
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:23
 */
public class SafeShutDown extends Thread{	
	@Override
	public void run(){
		if(GlobalParam.DISTRIBUTE_RUN) {
			if(EFNodeUtil.isMaster()) {
				GlobalParam.INSTANCE_COORDER.distributeCoorder().stopNodes();
			}else {
				try {
					GlobalParam.DISCOVERY_COORDER.leaveCluster(GlobalParam.IP, GlobalParam.NODEID);
				} catch (Exception e) {
					Common.LOG.warn("leave cluster failed, master is offline.");
				}
			}
		}else {
			stopAllInstances();
		}
	}
	
	public static void stopAllInstances() {
		String[] instances = GlobalParam.StartConfig.getProperty("instances").split(","); 
		for (int i = 0; i < instances.length; i++) {
			String[] strs = instances[i].split(":");
			if (strs.length <= 0 || strs[0].length() < 1)
				continue;
			if (Integer.parseInt(strs[1]) > 0) {
				GlobalParam.INSTANCE_COORDER.stopInstance(strs[0], GlobalParam.JOB_TYPE.FULL.name());
				GlobalParam.INSTANCE_COORDER.stopInstance(strs[0], GlobalParam.JOB_TYPE.FULL.name());
			}			
		}
	}
}
