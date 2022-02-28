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
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.node.EFNode;
import org.elasticflow.util.Common;
import org.elasticflow.yarn.EFRPCService;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.EFMonitorCoord;
import org.elasticflow.yarn.coord.InstanceCoord;
import org.elasticflow.yarn.coord.NodeCoord;

import com.alibaba.fastjson.JSONObject;

/**
 * master control running method
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public class DistributeInstanceCoorder {

	private int totalInstanceNum;

	private int avgInstanceNum;

	/** Check whether the system is initialized */
	private boolean isOnStart = true;

	private Lock rebalaceLock = new ReentrantLock();

	/** Threshold at which resources fall into scheduling **/
	private double cpuUsage = 85.;
	private double memUsage = 85.;

	private ArrayList<EFNode> nodes = new ArrayList<>();
			
	public void resumeInstance(String instance) {
		GlobalParam.INSTANCE_COORDER.resumeInstance(instance, GlobalParam.JOB_TYPE.INCREMENT.name());
		GlobalParam.INSTANCE_COORDER.resumeInstance(instance, GlobalParam.JOB_TYPE.FULL.name());
	}

	public void stopInstance(String instance) {
		GlobalParam.INSTANCE_COORDER.stopInstance(instance, GlobalParam.JOB_TYPE.INCREMENT.name());
		GlobalParam.INSTANCE_COORDER.stopInstance(instance, GlobalParam.JOB_TYPE.FULL.name());
	}	
	
	public void updateNodeConfigs(String instance, String end, String fieldName, String value) {
		nodes.forEach(n -> {
			if (n.containInstace(instance)) {
				n.getInstanceCoord().updateInstanceConfig(instance, end, fieldName, value);
			}
		});
	}

	public void updateAllNodesResource() {
		nodes.forEach(n -> {
			n.pushResource();
		});
	}

	public String getConnectionStatus(String instance, String poolName) {
		for (EFNode node : nodes) {
			if (node.containInstace(instance))
				return node.getEFMonitorCoord().getStatus(poolName);
		}
		return "";
	}

	public JSONObject getPipeEndStatus(String instance, String L1seq) {
		for (EFNode node : nodes) {
			if (node.containInstace(instance)) {
				JSONObject jo = node.getEFMonitorCoord().getPipeEndStatus(instance, L1seq);
				jo.put("nodeIP", node.getIp());
				jo.put("nodeID", node.getNodeId());
				return jo;
			}
		}
		JSONObject jo = new JSONObject();
		jo.put("nodeIP", "-");
		jo.put("nodeID", "-");
		jo.put("status", "offline");
		return jo;
	}

	public void updateNode(String ip, Integer nodeId) {
		if (!this.containsNode(nodeId)) {
			synchronized (nodes) {
				EFNode node = EFNode.getInstance(ip, nodeId);
				node.init(isOnStart,
						EFRPCService.getRemoteProxyObj(NodeCoord.class,
								new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT)),
						EFRPCService.getRemoteProxyObj(InstanceCoord.class,
								new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT)),
						EFRPCService.getRemoteProxyObj(EFMonitorCoord.class,
								new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT)),this);
				nodes.add(node);
			}
			Queue<String> bindInstances = clusterScan(false);
			if (nodes.size() >= GlobalParam.CLUSTER_MIN_NODES) {
				if (isOnStart) {
					isOnStart = false;
					this.rebalanceOnStart();
				} else {
					this.rebalanceOnNewNodeJoin(bindInstances);
				}
			}
		} else {
			this.getNode(nodeId).recoverInstance();
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
					Common.LOG.warn(
							"The cluster does not meet the conditions, all slave node tasks will be automatically closed.");
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

	/**
	 * Check cluster health
	 */
	public Queue<String> clusterScan(boolean startRebalace) {
		Queue<String> bindInstances = new LinkedList<String>();
		synchronized (nodes) {
			for (EFNode n : nodes) {
				if (!n.isLive()) {
					removeNode(n.getIp(), n.getNodeId(), false);
					bindInstances.addAll(n.getBindInstances());
				} else {
					n.refresh();
				}
			}
			if (!bindInstances.isEmpty() && startRebalace)
				this.rebalanceOnNodeLeave(bindInstances);
		}
		return bindInstances;
	}

	public boolean runClusterInstanceNow(String instance, String jobtype) {
		for (EFNode node : nodes) {
			if (node.containInstace(instance)) {
				return node.runInstanceNow(instance, jobtype);
			}
		}
		return false;
	}

	public synchronized void pushInstanceToCluster(String instanceSettting) {
		EFNode addNode = null;
		for (EFNode node : nodes) {
			if (addNode == null) {
				addNode = node;
			} else {
				if (node.getBindInstances().size() < addNode.getBindInstances().size()) {
					addNode = node;
				}
			}
		}
		addNode = this.demotionCheck(addNode);
		addNode.pushInstance(instanceSettting, false);
		totalInstanceNum += 1;
	}

	private EFNode demotionCheck(EFNode node) {
		EFNode _node = node;
		if (node.getCpuUsed() > this.cpuUsage || node.getMemUsed() > this.memUsage) {
			Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
			String lowestIntance = "";
			int lowestPriority = Integer.MAX_VALUE;
			for (Map.Entry<String, InstanceConfig> ents : configMap.entrySet()) {
				if (ents.getValue().getPipeParams().getPriority() < lowestPriority) {
					lowestPriority = ents.getValue().getPipeParams().getPriority();
					lowestIntance = ents.getKey();
				}
			}
			String pagesize = String.valueOf(
					Resource.nodeConfig.getInstanceConfigs().get(lowestIntance).getPipeParams().getReadPageSize() / 2);
			Resource.nodeConfig.getInstanceConfigs().get(lowestIntance).getPipeParams().setReadPageSize(pagesize);
			for (EFNode n : nodes) {
				if (n.containInstace(lowestIntance)) {
					n.getInstanceCoord().updateInstanceConfig(lowestIntance, "TransParam", "readPageSize", pagesize);
					_node = n;
				}
			}
		}
		return _node;
	}

	public synchronized void removeInstanceFromCluster(String instance) {
		for (EFNode node : nodes) {
			if (node.containInstace(instance)) {
				node.popInstance(instance);
				totalInstanceNum -= 1;
				break;
			}
		}
	}

	// ----------------other-------------------//
	private void rebalanceOnNodeLeave(Queue<String> bindInstances) {
		Common.LOG.info("node leave, start rebalance on {} nodes.", nodes.size());
		rebalaceLock.lock();
		int addNums = avgInstanceNum() - this.avgInstanceNum;
		if (addNums == 0)
			addNums = 1;
		Common.LOG.info("start NodeLeave distributing instance task.");
		while (!bindInstances.isEmpty()) {
			for (EFNode node : nodes) {
				node.pushInstance(bindInstances.poll(),true);
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
				node.pushInstance(idleInstances.poll(), true);
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
						node.pushInstance(instances[i], true);
					}
				}
				i++;
				if (i >= instances.length)
					break;
			}
		}
		this.avgInstanceNum = avgInstanceNum();
		rebalaceLock.unlock();
		Common.LOG.info("finish OnStart distributing instance task.");
	}

	private int avgInstanceNum() {
		return (int) Math.ceil(totalInstanceNum / (nodes.size()+0.0));
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
