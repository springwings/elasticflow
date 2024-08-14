package org.elasticflow.yarn.coordinator;

import org.elasticflow.util.Common;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.util.SystemInfoUtil;
import org.elasticflow.yarn.coord.master.NodeCoord;
import org.elasticflow.yarn.monitor.ResourceMonitor;

/**
 * Cluster node operation control Coordinator
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */

public class NodeCoordinator implements NodeCoord {

	@Override
	public void stopNode(boolean closeMoniter) {
		if (EFNodeUtil.isSlave() && closeMoniter) {
			ResourceMonitor.stop();
		}
		Common.stopSystem(false);
	}
	
	@Override
	public void restartNode(boolean closeMoniter) {
		if (EFNodeUtil.isSlave() && closeMoniter) {
			ResourceMonitor.stop();
		} 
		EFMonitorUtil.restartSystem();
	}
	
	@Override
	public double[] summaryResource() {
		return systemResource();
	}
	
	/**
	 * 
	 * @return double[], 0 cpu usage, 1 mem usage
	 */
	public static double[] systemResource() {
		double[] sum = new double[2];
		try {
			sum[0] = SystemInfoUtil.getCpuUsage();
			sum[1] = SystemInfoUtil.getMemUsage();
		} catch (Exception e) {
			Common.LOG.error("system resource usage summarize exception", e);
			sum[0] = 80.;
			sum[1] = 80.;
		}
		return sum;
	}
}
