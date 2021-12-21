package org.elasticflow.node;

import java.util.LinkedList;
import java.util.Queue;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.NodeConfig;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.yarn.coord.InstanceCoord;
import org.elasticflow.yarn.coord.NodeCoord;

/**
 * Node Model
 * 
 * @author chengwen
 * @version 1.0
 */
public class Node {

	private String ip;
	private boolean isLive=true;
	private int nodeId;
	/**instance Coordinator**/
	private InstanceCoord instanceCoord;
	/**node Coordinator**/
	private NodeCoord nodeCoord;
	/**node instances summarize **/
	private Queue<String> bindInstances = new LinkedList<String>();
	private long lastLiveTime;

	public static Node getInstance(String ip, Integer nodeId) {
		Node n = new Node(ip, nodeId);
		n.lastLiveTime = Common.getNow();
		return n;
	}

	public Node(String ip, Integer nodeId) {
		setIp(ip);
		setNodeId(nodeId);
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public boolean isLive() {
		if (Common.getNow() - this.lastLiveTime > GlobalParam.NODE_LIVE_TIME)
			setStatus(false);
		return isLive;
	}

	public void refresh() {
		this.lastLiveTime = Common.getNow();
	}

	public void setStatus(boolean isLive) {
		this.isLive = isLive;
	}

	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(Integer nodeId) {
		this.nodeId = nodeId;
	}

	public InstanceCoord getInstanceCoord() {
		return instanceCoord;
	}

	public void setInstanceCoord(InstanceCoord instanceCoord) {
		this.instanceCoord = instanceCoord;
	}

	public NodeCoord getNodeCoord() {
		return nodeCoord;
	}

	public void setNodeCoord(NodeCoord nodeCoord) {
		this.nodeCoord = nodeCoord;
	}

	public Queue<String> getBindInstances() {
		return bindInstances;
	}
	
	public String popInstance() {
		String instanceSetting = this.bindInstances.poll();
		if(instanceSetting!=null) {
			String[] strs = instanceSetting.split(":");
			this.instanceCoord.stopInstance(strs[0], GlobalParam.JOB_TYPE.INCREMENT.name());
			this.instanceCoord.stopInstance(strs[0], GlobalParam.JOB_TYPE.FULL.name());
			this.instanceCoord.removeInstance(strs[0]);
		}		
		return instanceSetting;
	}

	public void pushInstance(String instanceSetting) {
		this.bindInstances.offer(instanceSetting);
		String[] strs = instanceSetting.split(":");
		String[] paths = NodeConfig.getInstancePath(strs[0]);
		this.instanceCoord.sendInstanceData(EFFileUtil.readText(paths[0], "utf-8"),
				EFFileUtil.readText(paths[1], "utf-8"), strs[0]);
		this.instanceCoord.addInstance(instanceSetting);
	}
	
	public void pushResource() {
		String resource = GlobalParam.CONFIG_PATH + "/"+GlobalParam.StartConfig.getProperty("pond");
		String instructions = GlobalParam.CONFIG_PATH + "/"+GlobalParam.StartConfig.getProperty("instructions");
		this.instanceCoord.sendData(EFFileUtil.readText(resource, "utf-8"), "/"+GlobalParam.StartConfig.getProperty("pond"),true);
		this.instanceCoord.sendData(EFFileUtil.readText(instructions, "utf-8"), "/"+GlobalParam.StartConfig.getProperty("instructions"),true);
		this.instanceCoord.reloadResource();
	} 
	
	public void stopAllInstance() { 
		while(!this.bindInstances.isEmpty()) {
			popInstance();
		}
	}
}
