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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.elasticflow.yarn.coordinator.node.DistributeService;

import com.alibaba.fastjson.JSONObject;

/**
 * master control center
 * Control and acquisition sub node
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public class DistributeCoorder {

	private int totalInstanceNum;

	private int avgInstanceNum;
	
	/**cluster status :0 normal 1 freeze 2 down**/
	private AtomicInteger clusterStatus = new AtomicInteger(2);

	/** Check whether the system is initialized */
	private AtomicBoolean isOnStart = new AtomicBoolean(true);

	/** Threshold at which resources fall into scheduling **/
	private double cpuUsage = 90.;
	private double memUsage = 90.;
	
	private volatile Queue<Boolean> rebalanceQueue = new LinkedList<Boolean>();
	
	private ReentrantLock rebalanceLocker = new ReentrantLock();

	private CopyOnWriteArrayList<EFNode> nodes = new CopyOnWriteArrayList<>();
			
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
	
	public HashMap<String, Object> getNodeStatus() {
		HashMap<String, Object> res = new HashMap<>();
		for (EFNode node : nodes) {
			res.put(node.getIp(), node.getEFMonitorCoord().getNodeStatus());
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
				node.init(isOnStart.get(),
						EFRPCService.getRemoteProxyObj(NodeCoord.class,
								new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT)),
						EFRPCService.getRemoteProxyObj(InstanceCoord.class,
								new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT)),
						EFRPCService.getRemoteProxyObj(EFMonitorCoord.class,
								new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT)),this);
				nodes.add(node);
				Common.LOG.info("{} join cluster, current number of nodes is {}.",ip,nodes.size());
			} 
			if (clusterConditionMatch()) { 
				clusterStatus.set(0);
				if(rebalanceQueue.size()<2) {
					rebalanceQueue.add(true);
					rebalanceLocker.lock();
					while(!rebalanceQueue.isEmpty()) { 
						Queue<String> bindInstances = clusterScan(false);
						if (isOnStart.get()) {
							isOnStart.set(false);
							this.rebalanceOnStart();
						} else {
							this.rebalanceOnNewNodeJoin(bindInstances);
						}
						rebalanceQueue.poll();
					} 
					rebalanceLocker.unlock();
				}
				
			}
		} else {
			this.getNode(nodeId).recoverInstance();
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
			EFNode node = this.removeNode(nodeId);
			Queue<String> instances = new LinkedList<String>();
			instances.addAll(node.getBindInstances()); 
			Common.LOG.warn("ip {},nodeId {},leave cluster!",ip,nodeId); 
			node.stopAllInstance();
			if (!clusterConditionMatch()) {
				Common.LOG.warn("cluster not meet the requirements, all slave node tasks automatically closed."); 
				stopNodes(false);
			} else {
				if (rebalace)
					Resource.ThreadPools.execute(() -> {
						this.rebalanceOnNodeLeave(instances);
					});
			}
		}
	}

	public synchronized void stopNodes(boolean wait) {
		if(clusterStatus.get()!=1) {
			clusterStatus.set(1);			
			DistributeService.closeMonitor(); 
			CountDownLatch singal = new CountDownLatch(nodes.size());
			Resource.ThreadPools.execute(() -> { 
				nodes.forEach(n -> { 
					Common.LOG.info("node {},nodeId {}, is stopped!",n.getIp(),n.getNodeId());
					n.stopAllInstance();
					n.getNodeCoord().stopNode();
					singal.countDown();
				});
				nodes.clear();
				isOnStart.set(true);
				DistributeService.openMonitor();
				clusterStatus.set(2);
				Common.LOG.info("cluster all slave nodes are offline.");
			});
			if(wait) {
				try {
					singal.await();
				} catch (Exception e) {  
				}
			}
		}
	}

	/**
	 * Check cluster health
	 */
	public synchronized Queue<String> clusterScan(boolean startRebalace) {
		Queue<String> bindInstances = new LinkedList<String>();		
		for (EFNode n : nodes) {
			if(DistributeService.isOpenMonitor()) {
				if (!n.isLive()) {
					removeNode(n.getIp(), n.getNodeId(), false);
					bindInstances.addAll(n.getBindInstances());
				} else {
					n.refresh();
				}
			} 
		}
		if (DistributeService.isOpenMonitor() && !bindInstances.isEmpty() && startRebalace)
			this.rebalanceOnNodeLeave(bindInstances);
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
	private synchronized void rebalanceOnNodeLeave(Queue<String> bindInstances) {		
		if(!clusterConditionMatch()) {
			Common.LOG.warn("cluster not meet the requirements, all slave node tasks automatically closed."); 
			stopNodes(false);
		}else {
			this.avgInstanceNum = avgInstanceNum();
			Common.LOG.info("start NodeLeave rebalance on {} nodes,avgInstanceNum {}...", nodes.size(),avgInstanceNum);
			this.distributeInstances(bindInstances);
			Common.LOG.info("finish NodeLeave rebalance instance!");
		}
	}
 
	private void rebalanceOnNewNodeJoin(Queue<String> idleInstances) {
		synchronized (nodes) {
			this.avgInstanceNum = avgInstanceNum();
			Common.LOG.info("start NewNodeJoin rebalance on {} nodes, avgInstanceNum {}...", nodes.size(),avgInstanceNum);		
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
			this.distributeInstances(idleInstances);
		}
		Common.LOG.info("finish NewNodeJoin rebalance!");
	}

	private synchronized void rebalanceOnStart() {
		String[] instances = GlobalParam.StartConfig.getProperty("instances").split(",");
		Queue<String> runInstances = new LinkedList<String>();
		for (int i = 0; i < instances.length;i++) {
			String[] strs = instances[i].split(":");
			if (strs.length>1 && Integer.parseInt(strs[1]) > 0) {
				totalInstanceNum++; //summary run instance
				runInstances.add(instances[i]);
			}
		}
		Common.LOG.info("start cluster init rebalance, with {} nodes, instance total {}...", nodes.size(),totalInstanceNum);
		this.avgInstanceNum = avgInstanceNum();
		this.distributeInstances(runInstances);
		Common.LOG.info("finish cluster init rebalance!");
	}
	
	private void distributeInstances(Queue<String> runInstances) {
		if(runInstances.size()>0) {
			for (EFNode node : nodes) {
				while (node.getBindInstances().size() < avgInstanceNum) {
					if (runInstances.isEmpty())
						break;
					node.pushInstance(runInstances.poll(), true);
				}
			} 
		}
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
