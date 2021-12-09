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

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.config.NodeConfig;
import org.elasticflow.node.Node;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.yarn.EFRPCService;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.InstanceCoord;
import org.elasticflow.yarn.coord.NodeCoord;

/**
 * Run task instance cluster coordination operation The code runs on the client
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public class InstanceCoordinator implements InstanceCoord {

	private int totalInstanceNum;

	private int avgInstanceNum;

	private boolean isOnStart = true;

	volatile CopyOnWriteArrayList<Node> nodes = new CopyOnWriteArrayList<>();

	public void sendData(String content, String destination) {
		EFFileUtil.createFile(content, destination);
	}

	public void sendInstanceData(String content0, String content1, String instance) {
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
		EFMonitorUtil.rebuildFlowGovern(instanceSettting, true);
	}

	public void updateNode(String ip, Integer nodeId) {
		if (!this.containsNode(nodeId)) {
			synchronized (nodes) {
				Node node = Node.getInstance(ip, nodeId);
				node.setNodeCoord(EFRPCService.getRemoteProxyObj(NodeCoord.class,
						new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT)));
				node.setInstanceCoord(EFRPCService.getRemoteProxyObj(InstanceCoord.class,
						new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT)));
				nodes.add(node);
				if (nodes.size() >= GlobalParam.CLUSTER_MIN_NODES && isOnStart) {
					isOnStart = false;
					this.rebalanceOnStart();
				} else {
					this.rebalanceOnNewNodeJoin();
				}
			}
		} else {
			this.getNode(nodeId).refresh();
		}
	}

	public void removeNode(String ip, Integer nodeId, boolean rebalace) {
		synchronized (nodes) {
			if (this.containsNode(nodeId)) {
				Node node = this.removeNode(nodeId);
				if (nodes.size() < GlobalParam.CLUSTER_MIN_NODES) {
					nodes.forEach(n -> {
						n.getNodeCoord().stopNode();
					});
					Common.stopSystem();
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

	public void clusterScan() {
		Queue<String> bindInstances = new LinkedList<String>();
		synchronized (nodes) {
			for (Node n : nodes) {
				if (!n.isLive()) {
					removeNode(n.getIp(), n.getNodeId(), false);
					bindInstances.addAll(n.getBindInstances());
				}
			}
			if (!bindInstances.isEmpty())
				this.rebalanceOnNodeLeave(bindInstances);
		}

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

	private void rebalanceOnNodeLeave(Queue<String> bindInstances) {
		Common.LOG.info("node leave, start rebalance on {} nodes.", nodes.size());
		int addNums = avgInstanceNum() - this.avgInstanceNum;
		if (addNums == 0)
			addNums = 1;
		while (!bindInstances.isEmpty()) {
			for (Node node : nodes) {
				node.pushInstance(bindInstances.poll());
				if (bindInstances.isEmpty())
					break;
			}
		}
	}

	private void rebalanceOnNewNodeJoin() {
		Common.LOG.info("node join, start rebalance on {} nodes.", nodes.size());
		int avgInstanceNum = avgInstanceNum();
		this.avgInstanceNum = avgInstanceNum;
		int needNums = avgInstanceNum;
		Queue<String> idleInstances = new LinkedList<>();
		ArrayList<Node> addInstanceNodes = new ArrayList<>();
		for (Node node : nodes) {
			if (needNums == 0)
				break;
			if (node.getBindInstances().size() - avgInstanceNum < 0) {
				addInstanceNodes.add(node);
			} else {
				while (node.getBindInstances().size() - avgInstanceNum > 0) {
					idleInstances.offer(node.popInstance());
					needNums--;
					if (needNums == 0)
						break;
				}
			}
		}
		// re-balance nodes
		for (Node node : addInstanceNodes) {
			if (idleInstances.isEmpty())
				break;
			while (node.getBindInstances().size() - avgInstanceNum < 0) {
				node.pushInstance(idleInstances.poll());
			}
		}
	}

	private void rebalanceOnStart() {
		Common.LOG.info("start rebalance on {} nodes.", nodes.size());
		String[] instances = GlobalParam.StartConfig.getProperty("instances").split(",");
		for (int i = 0; i < instances.length; i++) {
			String[] strs = instances[i].split(":");
			if (strs.length <= 0 || strs[0].length() < 1)
				continue;
			if (Integer.parseInt(strs[1]) > 0) {
				for (Node node : nodes) {
					totalInstanceNum++;
					node.pushInstance(instances[i]);
					i++;
					if (i > instances.length - 1)
						break;
				}
			}
		}
		this.avgInstanceNum = avgInstanceNum();
	}

	private int avgInstanceNum() {
		int avg = totalInstanceNum / nodes.size();
		return (avg < 1) ? 1 : avg;
	}

	private Node getNode(Integer nodeId) {
		for (Node node : nodes) {
			if (node.getNodeId() == nodeId)
				return node;
		}
		return null;
	}

	private Node removeNode(Integer nodeId) {
		int removeNode = -1;
		for (int i = 0; i < nodes.size(); i++) {
			if (nodes.get(i).getNodeId() == nodeId) {
				removeNode = i;
				break;
			}
		}
		Node node = null;
		if (removeNode > 0) {
			node = nodes.get(removeNode);
			nodes.remove(removeNode);
		}
		return node;
	}

	private boolean containsNode(Integer nodeId) {
		for (Node node : nodes) {
			if (node.getNodeId() == nodeId)
				return true;
		}
		return false;
	}
}