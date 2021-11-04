package org.elasticflow.yarn.monitor;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-12-04 13:39
 */

public class ResourceMonitor {  
	
	public static void start() {
		new BlockMonitor().start();
	}
}
