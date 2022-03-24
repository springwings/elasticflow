package org.elasticflow.flow;

import org.elasticflow.util.Common;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coordinator.NodeCoordinator;

/**
 * EF Flow auto control center
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-31 10:52
 * @modify 2021-06-11 10:45
 */
public class EFlowMonitor {
	
	private boolean openRegulate = false;
	
	/** Threshold at which resources fall into scheduling **/
	private double cpuUsage = 92.;
	private double memUsage = 92.;
	
	
	
	public void checkResourceUsage() {
		float poolsize = Resource.ThreadPools.getPoolSize()+0.0f;
		int activate = Resource.ThreadPools.getActiveCount();
		for(int i=0;i<3;i++) {
			activate += Resource.ThreadPools.getActiveCount();
			try {
				Thread.sleep(100);
			} catch (Exception e) {
				Common.LOG.error(e.getMessage());
			}
		}
		double[] res = NodeCoordinator.systemResource();
		if(activate/(4*poolsize)>0.9 || res[0] > this.cpuUsage || res[1] > this.memUsage) {
			this.openRegulate = true;
		}else {
			this.openRegulate = false;
		}
	}

}
