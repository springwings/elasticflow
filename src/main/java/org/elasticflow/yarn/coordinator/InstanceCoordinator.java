/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coordinator;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.config.NodeConfig;
import org.elasticflow.node.EFNode;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.yarn.EFRPCService;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.EFMonitorCoord;
import org.elasticflow.yarn.coord.InstanceCoord;
import org.elasticflow.yarn.coord.NodeCoord;

import com.alibaba.fastjson.JSONObject;

/**
 * Run task instance cluster coordination operation The code runs on the slave/master
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public class InstanceCoordinator implements InstanceCoord {

	private int totalInstanceNum;

	private int avgInstanceNum;
	
	/**Check whether the system is initialized*/
	private boolean isOnStart = true;
		
	private Lock rebalaceLock = new ReentrantLock();

	volatile CopyOnWriteArrayList<EFNode> nodes = new CopyOnWriteArrayList<>();
	
	//----------------local running method-------------------//
	
	public void initNode(boolean isOnStart) {
		boolean wait = isOnStart;
		while(Resource.tasks.size()>0) {
			EFMonitorUtil.cleanAllInstance(wait);
			wait = false;
		}			
	}
	
	public void sendData(String content, String destination,boolean relative) {
		if(relative) {
			EFFileUtil.createFile(content, GlobalParam.CONFIG_PATH + destination);
		}else {
			EFFileUtil.createFile(content, destination);
		}		
	}

	public void reloadResource() {
		Resource.nodeConfig.parsePondFile(GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("pond"));
		Resource.nodeConfig.parseInstructionsFile(GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("instructions"));
	}
	
	public void sendInstanceData(String content0, String content1, String instance) {
		String[] paths = NodeConfig.getInstancePath(instance);
		sendData(content0, paths[0],false);
		sendData(content1, paths[1],false);
	}

	public void addInstance(String instanceSettting) {
		Resource.nodeConfig.loadConfig(instanceSettting, false);
		Resource.nodeConfig.loadInstanceConfig(instanceSettting);
		String tmp[] = instanceSettting.split(":");
		String instanceName = tmp[0];
		InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instanceName);
		if (instanceConfig.checkStatus())
			EFNodeUtil.initParams(instanceConfig);
		EFMonitorUtil.rebuildFlowGovern(instanceSettting, true);
	} 

	public void stopInstance(String instance, String jobtype) {
		if (jobtype.toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Stop, false);
		} else {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Stop, true);
		}
	}

	public void resumeInstance(String instance, String jobtype) {
		if (jobtype.toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Ready, false);
		} else {
			EFMonitorUtil.controlInstanceState(instance, STATUS.Ready, true);
		}
	}

	public void removeInstance(String instance,boolean waitComplete) {
		if(waitComplete)
			EFMonitorUtil.controlInstanceState(instance, STATUS.Stop, true);
		if (Resource.nodeConfig.getInstanceConfigs().get(instance).getInstanceType() > 0) {
			Resource.FLOW_INFOS.remove(instance, JOB_TYPE.FULL.name());
			Resource.FLOW_INFOS.remove(instance, JOB_TYPE.INCREMENT.name());
		}		
		Resource.FlOW_CENTER.removeInstance(instance, true, true);
		EFMonitorUtil.removeConfigInstance(instance);
	} 
	
	//----------------master control running method-------------------//
	public void updateAllNodesResource() {
		nodes.forEach(n -> {
			n.pushResource();
		});
	}

	public String getConnectionStatus(String instance,String poolName) {
		for (EFNode node : nodes) { 
			if(node.containInstace(instance))
				return node.getEFMonitorCoord().getStatus(poolName);
		}
		return "";
	}
	
	public JSONObject getPipeEndStatus(String instance,String L1seq) {
		for (EFNode node : nodes) { 
			if(node.containInstace(instance)) {
				JSONObject jo = node.getEFMonitorCoord().getPipeEndStatus(instance,L1seq);
				jo.put("nodeIP", node.getIp());
				jo.put("nodeID", node.getNodeId());
				return jo;
			}
		}
		return new JSONObject();
	}

	public void updateNode(String ip, Integer nodeId) {
		if (!this.containsNode(nodeId)) {
			synchronized (nodes) { 
				EFNode node = EFNode.getInstance(ip, nodeId);
				node.init(isOnStart,EFRPCService.getRemoteProxyObj(NodeCoord.class,
						new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT)), 
						EFRPCService.getRemoteProxyObj(InstanceCoord.class,
								new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT)),
						EFRPCService.getRemoteProxyObj(EFMonitorCoord.class,
								new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT)));	
				nodes.add(node);
			}
			Queue<String> bindInstances = clusterScan(false);
			if(nodes.size() >= GlobalParam.CLUSTER_MIN_NODES) {
				if (isOnStart) {
					isOnStart = false;
					this.rebalanceOnStart();
				} else {
					this.rebalanceOnNewNodeJoin(bindInstances);
				}
			}	
		} else {
			this.getNode(nodeId).refresh();
		}
	}

	public void removeNode(String ip, Integer nodeId, boolean rebalace) {
		synchronized (nodes) {
			if (this.containsNode(nodeId)) {
				EFNode node = this.removeNode(nodeId);
				if (nodes.size() < GlobalParam.CLUSTER_MIN_NODES) {
					nodes.forEach(n -> {
						n.getNodeCoord().stopNode();
					});
					isOnStart = true;
					Common.LOG.warn("The cluster does not meet the conditions, all slave node tasks will be automatically closed.");
				} else {
					if (rebalace)
						this.rebalanceOnNodeLeave(node.getBindInstances());
				}
			}
		}
	}

	public void stopNodes() {
		nodes.forEach(n -> {
			n.stopAllInstance();
			n.getNodeCoord().stopNode();
		});
	}

	public Queue<String> clusterScan(boolean startRebalace) {
		Queue<String> bindInstances = new LinkedList<String>();
		synchronized (nodes) {
			for (EFNode n : nodes) {
				if (!n.isLive()) {
					removeNode(n.getIp(), n.getNodeId(), false);
					bindInstances.addAll(n.getBindInstances());
				}
			}
			if (!bindInstances.isEmpty() && startRebalace)
				this.rebalanceOnNodeLeave(bindInstances);
		}
		return bindInstances;
	} 

	//----------------other-------------------//
	private void rebalanceOnNodeLeave(Queue<String> bindInstances) {
		Common.LOG.info("node leave, start rebalance on {} nodes.", nodes.size());
		rebalaceLock.lock();
		int addNums = avgInstanceNum() - this.avgInstanceNum;
		if (addNums == 0)
			addNums = 1;
		Common.LOG.info("start NodeLeave distributing instance task.");
		while (!bindInstances.isEmpty()) {
			for (EFNode node : nodes) {
				node.pushInstance(bindInstances.poll(),this);
				if (bindInstances.isEmpty())
					break;
			}
		}
		rebalaceLock.unlock();
		Common.LOG.info("finish NodeLeave distributing instance task.");
	}

	private void rebalanceOnNewNodeJoin(Queue<String> idleInstances) {
		Common.LOG.info("node join, start rebalance on {} nodes.", nodes.size());
		rebalaceLock.lock();
		int avgInstanceNum = avgInstanceNum();
		this.avgInstanceNum = avgInstanceNum;
		ArrayList<EFNode> addInstanceNodes = new ArrayList<>();
		for (EFNode node : nodes) {
			if (node.getBindInstances().size() < avgInstanceNum) {
				addInstanceNodes.add(node);
			} else {
				while (node.getBindInstances().size() > avgInstanceNum) {					
					idleInstances.offer(node.popInstance());
				}
			}
		}
		// re-balance nodes
		Common.LOG.info("start NewNodeJoin distributing instance task.");
		for (EFNode node : addInstanceNodes) {
			if (idleInstances.isEmpty())
				break;
			while (node.getBindInstances().size() < avgInstanceNum) { 
				node.pushInstance(idleInstances.poll(),this);
			}
		}
		rebalaceLock.unlock();
		Common.LOG.info("finish NewNodeJoin distributing instance task.");
	}

	private void rebalanceOnStart() {
		Common.LOG.info("start rebalance on {} nodes.", nodes.size());
		rebalaceLock.lock();
		String[] instances = GlobalParam.StartConfig.getProperty("instances").split(",");
		Common.LOG.info("start OnStart distributing instance task.");
		for (int i = 0; i < instances.length;) {
			for (EFNode node : nodes) {
				String[] strs = instances[i].split(":");
				if (strs.length > 1 && strs[0].length() > 1) { 
					if (Integer.parseInt(strs[1]) > 0) {
						totalInstanceNum++;
						node.pushInstance(instances[i],this);
						if (i > instances.length - 1)
							break;
					}	
				}	
				i++;
			}			
		}
		this.avgInstanceNum = avgInstanceNum();
		rebalaceLock.unlock();
		Common.LOG.info("finish OnStart distributing instance task.");
	}

	private int avgInstanceNum() {
		int avg = totalInstanceNum / nodes.size();
		return (avg < 1) ? 1 : avg;
	}

	private EFNode getNode(Integer nodeId) {
		for (EFNode node : nodes) {
			if (node.getNodeId() == nodeId)
				return node;
		}
		return null;
	}
	
	

	private EFNode removeNode(Integer nodeId) {
		int removeNode = -1;
		for (int i = 0; i < nodes.size(); i++) {
			if (nodes.get(i).getNodeId() == nodeId) {
				removeNode = i;
				break;
			}
		}
		EFNode node = null;
		node = nodes.get(removeNode);
		nodes.remove(removeNode);
		return node;
	}

	private boolean containsNode(Integer nodeId) {
		for (EFNode node : nodes) {
			if (node.getNodeId() == nodeId)
				return true;
		}
		return false;
	} 
}
