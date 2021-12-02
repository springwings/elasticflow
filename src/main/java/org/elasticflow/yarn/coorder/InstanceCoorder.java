/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coorder;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.config.NodeConfig;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.node.Node;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.yarn.EFRPCClient;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.InstanceCoord;


/**
 * Run task instance cluster coordination operation
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public class InstanceCoorder implements InstanceCoord{
	
	volatile ConcurrentHashMap<Integer,Node> nodes = new ConcurrentHashMap<>();
	
	volatile HashMap<String, Integer> instanceNode = new HashMap<>();
	
	volatile ConcurrentHashMap<Integer, InstanceCoord> instanceCoord = new ConcurrentHashMap<Integer, InstanceCoord>();
	
	public void sendData(String content, String destination) {
		EFFileUtil.createFile(content, destination);
	}
	
	public void sendInstanceData(String content0,String content1, String instance) {
		String[] paths = NodeConfig.getInstancePath(instance); 
		sendData(content0, paths[0]);
		sendData(content1, paths[1]);
	}
	
	public void addInstance(String instanceSettting) {
		Resource.nodeConfig.loadConfig(instanceSettting, false);
		Resource.nodeConfig.loadInstanceConfig(instanceSettting);
		String tmp[] = instanceSettting.split(":");
		String instanceName = tmp[0];
		InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instanceName);
		if (instanceConfig.checkStatus())
			EFNodeUtil.initParams(instanceConfig);
		EFMonitorUtil.rebuildFlowGovern(instanceSettting,true);		
	}
	
	public void addNode(String ip,String nodeId) {
		Integer id = Integer.parseInt(nodeId);
		if(!nodes.containsKey(id)) {
			nodes.put(id,Node.getInstance(ip, nodeId));
			this.rebalanceInstances();
		}			
	}
	
	public void rebalanceInstances() {
		if(nodes.size()>=Integer.parseInt(GlobalParam.StartConfig.getProperty("min_nodes"))) {
			Common.LOG.info("rebalance instances over nodes.");
			String[] _instances = GlobalParam.StartConfig.getProperty("instances").split(",");
			int nodeSize = nodes.size();
			for(int i=0;i<_instances.length;i++) {
				String[] strs = _instances[i].split(":");
				if (strs.length <= 0 || strs[0].length() < 1)
					continue;
				if(Integer.parseInt(strs[1])>0) {
					Integer nodeid = i/nodeSize+1;
					instanceNode.put(_instances[i],nodeid);
					synchronized(instanceCoord) {
						if(!instanceCoord.contains(nodeid)) {
							instanceCoord.put(nodeid, EFRPCClient.getRemoteProxyObj(InstanceCoord.class, 
								new InetSocketAddress(nodes.get(nodeid).getIp(), GlobalParam.NODE_INSTANCE_SYN_PORT)));
						} 
					}					
					String[] paths = NodeConfig.getInstancePath(strs[0]); 
					instanceCoord.get(nodeid).sendInstanceData(EFFileUtil.readText(paths[0], "utf-8"),
							EFFileUtil.readText(paths[1], "utf-8"),strs[0]);
					instanceCoord.get(nodeid).addInstance(_instances[i]);
				}
			}			
		}else {
			Common.LOG.warn("The number of start slave nodes does not meet the min_nodes requirements.");
		}		
	} 
	
	public void stopInstance(String instance,String jobtype) {
		if (jobtype.toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Stop, false);
		} else {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Stop, true);
		}
	}
	
	public void resumeInstance(String instance,String jobtype) {
		if (jobtype.toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Ready, false);
		} else {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Ready, true);
		}
	}
	
	public void removeInstance(String instance) {
		EFMonitorUtil.controlInstanceState(instance, STATUS.Stop, true);
		if (Resource.nodeConfig.getInstanceConfigs().get(instance).getInstanceType() > 0) {
			Resource.FLOW_INFOS.remove(instance, JOB_TYPE.FULL.name());
			Resource.FLOW_INFOS.remove(instance, JOB_TYPE.INCREMENT.name());
		}
		Resource.nodeConfig.getInstanceConfigs().remove(instance);
		Resource.FlOW_CENTER.removeInstance(instance, true, true);
		EFMonitorUtil.removeConfigInstance(instance);
	}
	
}
