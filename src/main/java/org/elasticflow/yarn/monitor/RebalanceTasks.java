package org.elasticflow.yarn.monitor;

import java.util.HashMap;
import java.util.Map;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.yarn.Resource;

/**
* Rebalance Tasks over all nodes.
* @author chengwen
* @version 2.0
* @date 2018-11-21 15:43
*/
public class RebalanceTasks {
	
	HashMap<String,Integer> instances = new HashMap<>();
	
	public void reloadConfigs() {
		Map<String, InstanceConfig> nodes = Resource.nodeConfig.getInstanceConfigs();
		for (Map.Entry<String, InstanceConfig> entry : nodes.entrySet()) {
			
		}
	}

}
