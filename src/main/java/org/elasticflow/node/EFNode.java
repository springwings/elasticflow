package org.elasticflow.node;

import java.util.LinkedList;
import java.util.Queue;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.NodeConfig;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.yarn.coord.EFMonitorCoord;
import org.elasticflow.yarn.coord.InstanceCoord;
import org.elasticflow.yarn.coord.NodeCoord;
import org.elasticflow.yarn.coordinator.InstanceCoordinator;

/**
 * Node Model
 * It is mainly used for distributed node control
 * @author chengwen
 * @version 1.0
 */
public class EFNode {

	private String ip;
	private boolean isLive = true;
	private int nodeId;
	/** slave instance Coordinator **/
	private InstanceCoord instanceCoord;
	/** slave node monitor Coordinator **/
	private EFMonitorCoord monitorCoord;
	/** slave node Coordinator **/
	private NodeCoord nodeCoord;
	/** node instances configure summarize **/
	private volatile Queue<String> bindInstances = new LinkedList<String>();
	private long lastLiveTime;

	public static EFNode getInstance(String ip, Integer nodeId) {
		EFNode n = new EFNode(ip, nodeId);
		n.lastLiveTime = Common.getNow();
		return n;
	}

	public EFNode(String ip, Integer nodeId) {
		setIp(ip);
		setNodeId(nodeId);
	}

	public void init(boolean isOnStart,NodeCoord nodeCoord, InstanceCoord instanceCoord, EFMonitorCoord monitorCoord) {
		this.nodeCoord = nodeCoord;
		this.instanceCoord = instanceCoord;
		this.monitorCoord = monitorCoord;
		this.instanceCoord.initNode(isOnStart);
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

	public EFMonitorCoord getEFMonitorCoord() {
		return monitorCoord;
	}

	public NodeCoord getNodeCoord() {
		return nodeCoord;
	}

	public Queue<String> getBindInstances() {
		return bindInstances;
	}

	public boolean containInstace(String instance) {
		for (String instanceSetting : bindInstances) {
			if (instanceSetting.split(":")[0].equals(instance))
				return true;
		}
		return false;
	}

	public String popInstance() {
		String instanceSetting = this.bindInstances.poll();
		if (instanceSetting != null) {
			String[] strs = instanceSetting.split(":");
			this.instanceCoord.stopInstance(strs[0], GlobalParam.JOB_TYPE.INCREMENT.name());
			this.instanceCoord.stopInstance(strs[0], GlobalParam.JOB_TYPE.FULL.name());
			this.instanceCoord.removeInstance(strs[0], true);
		}
		return instanceSetting;
	}
	
	public void recoverInstance(InstanceCoordinator instanceCoordinator) {
		if(this.instanceCoord.onlineTasksNum()==0) {
			for(String instanceSetting : this.bindInstances) {
				this.pushInstance(instanceSetting, instanceCoordinator, false);
			}
		}		
	}

	public void pushInstance(String instanceSetting,InstanceCoordinator instanceCoordinator,boolean updateBindInstances) {
		if(updateBindInstances)
			this.bindInstances.offer(instanceSetting);
		String[] strs = instanceSetting.split(":");
		String[] paths = NodeConfig.getInstancePath(strs[0]);
		this.instanceCoord.sendInstanceData(EFFileUtil.readText(paths[0], "utf-8"),
				EFFileUtil.readText(paths[1], "utf-8"), strs[0]);
		this.instanceCoord.addInstance(instanceSetting);
		instanceCoordinator.resumeInstance(strs[0], GlobalParam.JOB_TYPE.INCREMENT.name());
		instanceCoordinator.resumeInstance(strs[0], GlobalParam.JOB_TYPE.FULL.name());
	}

	public void pushResource() {
		String resource = GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("pond");
		String instructions = GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("instructions");
		this.instanceCoord.sendData(EFFileUtil.readText(resource, "utf-8"),
				"/" + GlobalParam.StartConfig.getProperty("pond"), true);
		this.instanceCoord.sendData(EFFileUtil.readText(instructions, "utf-8"),
				"/" + GlobalParam.StartConfig.getProperty("instructions"), true);
		this.instanceCoord.reloadResource();
	}

	public void stopAllInstance() {
		while (!this.bindInstances.isEmpty()) {
			popInstance();
		}
	}
}
