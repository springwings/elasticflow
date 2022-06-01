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
public class EFlowMonitor{
	
	private boolean openRegulate = false;
	
	/** Threshold at which resources fall into scheduling **/
	private double cpuUsage = 92.;
	private double memUsage = 92.;
	
	private double resourceAbundance = 1.f;
	
	public boolean isOpenRegulate() {
		return openRegulate;
	}

	public double getResourceAbundance() {
		return resourceAbundance;
	}

	public void checkResourceUsage() {
		float poolsize = Resource.threadPools.getPoolSize()+0.0f;
		int activate = Resource.threadPools.getActiveCount();
		for(int i=0;i<3;i++) {
			activate += Resource.threadPools.getActiveCount();
			try {
				Thread.sleep(100);
			} catch (Exception e) {
				Common.LOG.error(e.getMessage());
			}
		}
		double[] res = NodeCoordinator.systemResource();
		float threadRate = activate/(4*poolsize);
		if(threadRate>0.9 || res[0] > this.cpuUsage || res[1] > this.memUsage) {
			this.openRegulate = true;
		}else {
			this.openRegulate = false;
		}
		this.resourceAbundanceCheck(1-threadRate, 1-res[0], 1-res[1]);
	}
	
	private void resourceAbundanceCheck(float threadIdleRate,double cpuIdlePercent,double memIdlePercent) {
		resourceAbundance = (threadIdleRate*100.0*0.8+cpuIdlePercent*0.15+memIdlePercent*0.05)/100.0;
	}

}
