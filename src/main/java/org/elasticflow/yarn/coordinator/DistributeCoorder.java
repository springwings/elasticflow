/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coordinator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.node.EFNode;
import org.elasticflow.util.Common;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coordinator.node.DistributeService;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * master control center Control and acquisition sub node
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public class DistributeCoorder {

	private int totalInstanceNum;

	private int avgInstanceNum;

	private volatile AtomicInteger rebalanceCount = new AtomicInteger();

	/** cluster status :0 normal, 1 freeze, 2 rebalance, 3 down **/
	private AtomicInteger clusterStatus = new AtomicInteger(1);

	/** Check whether the system is initialized */
	private AtomicBoolean isOnStart = new AtomicBoolean(true);

	/** Threshold at which resources fall into scheduling **/
	private double cpuUsage = 90.;
	private double memUsage = 90.;

	private CopyOnWriteArrayList<EFNode> nodes = new CopyOnWriteArrayList<>();
	
	private ConcurrentLinkedQueue<String> idleInstances = new ConcurrentLinkedQueue<>();
	
	public DistributeCoorder() {
		String[] instances = GlobalParam.StartConfig.getProperty("instances").split(",");
		for (int i = 0; i < instances.length; i++) {
			String[] strs = instances[i].split(":");
			if (strs.length > 1 && Integer.parseInt(strs[1]) > 0) {
				totalInstanceNum++; // summary run instance
				this.idleInstances.add(instances[i]);
			}
		}
	}

	/** control master instance local state */
	public void resumeInstance(String instance) {
		GlobalParam.INSTANCE_COORDER.resumeInstance(instance, GlobalParam.JOB_TYPE.INCREMENT.name());
		GlobalParam.INSTANCE_COORDER.resumeInstance(instance, GlobalParam.JOB_TYPE.FULL.name());
	}	

	/** control master instance local state */
	public void stopInstance(String instance) {
		GlobalParam.INSTANCE_COORDER.stopInstance(instance, GlobalParam.JOB_TYPE.INCREMENT.name());
		GlobalParam.INSTANCE_COORDER.stopInstance(instance, GlobalParam.JOB_TYPE.FULL.name());
	}

	public String getClusterState() {
		switch (clusterStatus.get()) {
		case 0:
			return "normal";
		case 1:
			return "freeze";
		case 2:
			return "rebalance";
		default:
			return "down";
		}
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

	public HashMap<String, Object> getNodeStatus() {
		HashMap<String, Object> res = new HashMap<>();
		for (EFNode node : nodes) {
			if(node.isLive()) {
				res.put(node.getIp(), node.getEFMonitorCoord().getNodeStatus());
			}			
		}
		return res;
	}

	public String getConnectionStatus(String instance, String poolName) {
		for (EFNode node : nodes) {
			if (node.containInstace(instance))
				return node.getEFMonitorCoord().getPoolStatus(poolName);
		}
		return "";
	}

	public void resetBreaker(String instance, String L1seq) {
		for (EFNode node : nodes) {
			if (node.containInstace(instance))
				node.resetBreaker(instance, L1seq);
		}
	}

	public JSONObject getBreakerStatus(String instance, String L1seq, String appendPipe) {
		for (EFNode node : nodes) {
			if (node.containInstace(instance) && node.isLive()) {
				return node.getBreakerStatus(instance, L1seq, appendPipe);
			}
		}
		JSONObject JO = new JSONObject();
		JO.put(appendPipe + "breaker_is_on",
				Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.isOn());
		if (Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.isOn()) {
			JO.put(appendPipe + "breaker_is_on_reason",
					Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.getReason());
		}
		JO.put(appendPipe + "valve_turn_level",
				Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).valve.getTurnLevel());
		JO.put(appendPipe + "current_fail_freq",
				Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.failInterval());
		JO.put(appendPipe + "total_fail_times",
				Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.getFailTimes());
		return JO;
	}

	public JSONObject getPipeEndStatus(String instance, String L1seq) {
		for (EFNode node : nodes) {
			if (node.containInstace(instance) && node.isLive()) {
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

	/**
	 * update Cluster nodes live status
	 * 
	 * @param ip
	 * @param nodeId
	 */
	public void updateCluster(String ip, Integer nodeId) {
		if (!this.containsNode(nodeId)) {
			synchronized (nodes) {
				EFNode node = EFNode.getInstance(ip, nodeId);
				node.init(this, true);
				nodes.add(node);
				Common.LOG.info("{} join cluster, current number of nodes is {}.", ip, nodes.size());
				rebalanceCount.incrementAndGet();
				if (isOnStart.get())
					clusterStatus.set(2);
			}
			// cluster Steady checking
			try {
				Thread.sleep(9000);
			} catch (InterruptedException e) {
				Common.stopSystem(false);
			}
			synchronized (rebalanceCount) {
				if (rebalanceCount.decrementAndGet() == 0) {
					if (clusterConditionMatch()) {						
						if (isOnStart.get()) {
							isOnStart.set(false);
							this.rebalanceOnStart();							
						} else {
							this.rebalanceOnNewNodeJoin();
						}						
					}
				}
			}

		} else {
			//Flash section recovery
			if(this.getNode(nodeId).needRecover()) {
				synchronized (nodes) {
					EFNode node = EFNode.getInstance(ip, nodeId);
					node.init(this, true);
					node.getBindInstances().addAll(this.getNode(nodeId).getBindInstances());
					this.removeNode(nodeId);
					nodes.add(node);
					node.recoverInstance();
				}
			}
			this.getNode(nodeId).refresh();
		}
	}

	private boolean clusterConditionMatch() {
		if (nodes.size() < GlobalParam.CLUSTER_MIN_NODES) {
			return false;
		}
		return true;
	}

	public synchronized void removeNode(String ip, Integer nodeId, boolean rebalace) {
		if (this.containsNode(nodeId)) {
			clusterStatus.set(2);
			EFNode node = this.removeNode(nodeId);
			idleInstances.addAll(node.getBindInstances());
			Common.LOG.warn("ip {},nodeId {},leave cluster!", ip, nodeId);
			if (!clusterConditionMatch()) {
				Common.LOG.warn("cluster not meet the requirements, all slave node tasks automatically closed.");
				stopAllNodes(false);
			} else {
				if (rebalace)
					Resource.threadPools.execute(() -> {
						this.rebalanceOnNodeLeave();
					});
			}
		}
	}
	
	/**
	 * 
	 * @param wait, asyn or syn
	 */
	public synchronized void stopAllNodes(boolean wait) {
		if (clusterStatus.get() != 1) {
			clusterStatus.set(1);
			DistributeService.closeMonitor();
			CountDownLatch singal = new CountDownLatch(nodes.size());
			Resource.threadPools.execute(() -> {
				nodes.forEach(n -> {
					Common.LOG.info("node {},nodeId {}, is stopped!", n.getIp(), n.getNodeId());
					try {
						n.stopAllInstance();
						n.getNodeCoord().stopNode();
					}finally {
						singal.countDown();
					}
				});
				nodes.clear();
				isOnStart.set(true);
				DistributeService.openMonitor();
				clusterStatus.set(3);
				Common.LOG.info("cluster all slave nodes are offline.");
			});
			if (wait) {
				try {
					singal.await(300,TimeUnit.SECONDS);
				} catch (Exception e) {
					Common.LOG.error(e.getMessage());
				}
			}
		}
	}

	/**
	 * Check cluster health
	 */
	public synchronized void clusterScan() {
		boolean nodeLeave = false;
		for (EFNode n : nodes) {
			if (DistributeService.isOpenMonitor()) {
				if (!n.isLive()) {
					removeNode(n.getIp(), n.getNodeId(), false);
					nodeLeave = true;
				} else {
					n.refresh();
				}
			}
		}
		if (DistributeService.isOpenMonitor() && nodeLeave)
			this.rebalanceOnNodeLeave();		
	}

	public boolean runClusterInstanceNow(String instance, String jobtype, boolean asyn) {
		for (EFNode node : nodes) {
			if (node.containInstace(instance)) {
				return node.runInstanceNow(instance, jobtype, asyn);
			}
		}
		return false;
	}
	
	public synchronized void reloadClusterInstance(String instanceSettting,boolean reset) {
		String[] strs = instanceSettting.split(":");
		for (EFNode node : nodes) {
			if(node.containInstace(strs[0])) {
				node.pushInstance(instanceSettting, false);
				break;
			}
		}
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
		if (addNode != null) {
			addNode = this.demotionCheck(addNode);
			addNode.pushInstance(instanceSettting, false);
			totalInstanceNum += 1;
		}
	}
	
	/**
	 * Priority check control
	 * @param node
	 * @return
	 */
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

	public synchronized void removeInstanceFromCluster(String instance,boolean keepInNode) {
		for (EFNode node : nodes) {
			if (node.containInstace(instance)) {
				if(keepInNode) {
					node.stopInstance(instance);
				}else {
					node.popInstance(instance);
					totalInstanceNum -= 1;
				}				
				break;
			}
		}
	}

	// ----------------other-------------------//
	private synchronized void rebalanceOnNodeLeave() {
		if (!clusterConditionMatch()) {
			Common.LOG.warn("cluster not meet the requirements, all slave node tasks automatically closed.");
			stopAllNodes(false);
		} else {
			this.avgInstanceNum = avgInstanceNum();
			Common.LOG.info("start NodeLeave rebalance on {} nodes,avgInstanceNum {}...", nodes.size(), avgInstanceNum);
			this.distributeInstances();			
			clusterStatus.set(0);
			this.storeNodesStatus();
			Common.LOG.info("finish NodeLeave rebalance instance!");
		}
	}

	private synchronized void rebalanceOnNewNodeJoin() {
		clusterStatus.set(2);
		this.avgInstanceNum = avgInstanceNum();
		Common.LOG.info("start NewNodeJoin rebalance on {} nodes, avgInstanceNum {}...", nodes.size(),
				avgInstanceNum);
		for (EFNode node : nodes) {
			while (node.getBindInstances().size() > avgInstanceNum) {
				idleInstances.offer(node.popInstance());
			}
		}
		// re-balance nodes
		this.distributeInstances();
		clusterStatus.set(0);
		this.storeNodesStatus();
		Common.LOG.info("finish NewNodeJoin rebalance!");
	}

	private synchronized void rebalanceOnStart() {		
		Common.LOG.info("start cluster init rebalance, with {} nodes, total number of instances is {}...", nodes.size(),
				totalInstanceNum);
		this.avgInstanceNum = avgInstanceNum();
		this.distributeInstances();
		this.storeNodesStatus();
		clusterStatus.set(0);
		Common.LOG.info("finish cluster init rebalance!");
	}

	private void storeNodesStatus() {
		String fpath = GlobalParam.CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID + "/status";
		JSONArray JA = new JSONArray();
		for (EFNode node : nodes) {
			JA.add(node.getNodeInfos());
		}
		EFDataStorer.setData(fpath, JSON.toJSONString(JA));
	}

	private boolean containInstanceLocation(int nodeId) {
		for (EFNode node : nodes) {
			if (node.getNodeId() == nodeId)
				return true;
		}
		return false;
	}

	/**
	 * distribute Instances over cluster
	 * 
	 * @param runInstances
	 */
	private void distributeInstances() {
		if (this.idleInstances.size() > 0) {
			Queue<String> keepInstances = new LinkedList<String>();
			synchronized (nodes) {
				EFNode removeNode = null;
				for (EFNode node : nodes) {
					while (node.getBindInstances().size() < avgInstanceNum) {
						if (this.idleInstances.isEmpty())
							break;
						try {
							String instance = this.idleInstances.poll();
							if (Resource.nodeConfig.getInstancesLocation().containsKey(instance)
									&& this.containInstanceLocation(
											Resource.nodeConfig.getInstancesLocation().get(instance))) {
								if (node.getNodeId() == Resource.nodeConfig.getInstancesLocation().get(instance)) {
									node.pushInstance(instance, true);
								} else {
									keepInstances.add(instance);
								}
							} else {
								node.pushInstance(instance, true);
							}
						} catch (Exception e) {
							removeNode = node;
							break;
						}
					}
					if (removeNode != null)
						break;
				}
				if (removeNode != null) {
					if (nodes.contains(removeNode)) {
						nodes.remove(removeNode);
					}
					this.avgInstanceNum = avgInstanceNum();
					distributeInstances();
				}
			}
			while (keepInstances.peek() != null) {
				String instance = keepInstances.poll();
				for (EFNode node : nodes) {
					if (node.getNodeId() == Resource.nodeConfig.getInstancesLocation().get(instance)) {
						node.pushInstance(instance, true);
						break;
					}
				}
			}
		}
	}

	private int avgInstanceNum() {
		return (int) Math.ceil(totalInstanceNum / (nodes.size() + 0.0));
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
