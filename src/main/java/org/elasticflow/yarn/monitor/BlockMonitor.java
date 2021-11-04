package org.elasticflow.yarn.monitor;

import java.util.HashMap;
import java.util.Map;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.yarn.Resource;

public class BlockMonitor extends Thread {		
	HashMap<String,Integer> instances = new HashMap<>();	
	
	public BlockMonitor() {
		Map<String, InstanceConfig> nodes = Resource.nodeConfig.getInstanceConfigs();
		for (Map.Entry<String, InstanceConfig> entry : nodes.entrySet()) {
			InstanceConfig config = entry.getValue();
			instances.put(entry.getKey(), config.getInstanceType());
		}
	}		
    @Override
    public void run() {
    
    }
}