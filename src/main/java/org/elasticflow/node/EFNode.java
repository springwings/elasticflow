package org.elasticflow.node;

import java.util.LinkedList;
import java.util.Queue;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.yarn.coord.EFMonitorCoord;
import org.elasticflow.yarn.coord.InstanceCoord;
import org.elasticflow.yarn.coord.NodeCoord;
import org.elasticflow.yarn.coordinator.DistributeCoorder;

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
	private double cpuUsed = 80.;
	private double memUsed = 80.;
	/** slave instance Coordinator **/
	private InstanceCoord instanceCoord;
	/** slave node monitor Coordinator **/
	private EFMonitorCoord monitorCoord;
	/** slave node Coordinator **/
	private NodeCoord nodeCoord;
	/** node instances configure summarize **/
	private volatile DistributeCoorder masterInstanceCoorder;
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

	public void init(boolean isOnStart,NodeCoord nodeCoord, InstanceCoord instanceCoord, 
			EFMonitorCoord monitorCoord,DistributeCoorder masterInstanceCoorder) {
		this.nodeCoord = nodeCoord;
		this.instanceCoord = instanceCoord;
		this.monitorCoord = monitorCoord;
		this.instanceCoord.initNode(isOnStart);
		this.masterInstanceCoorder = masterInstanceCoorder;
		this.pushResource();
		this.stopAllInstance();
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
		double tmp[] = nodeCoord.summaryResource();
		this.cpuUsed = tmp[0];
		this.memUsed = tmp[1];
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
	
	public double getCpuUsed() {
		return cpuUsed;
	}

	public void setCpuUsed(double cpuUsed) {
		this.cpuUsed = cpuUsed;
	}

	public double getMemUsed() {
		return memUsed;
	}

	public void setMemUsed(double memUsed) {
		this.memUsed = memUsed;
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
	
	public void recoverInstance() {
		if(this.instanceCoord.onlineTasksNum()==0) {
			for(String instanceSetting : this.bindInstances) {
				this.pushInstance(instanceSetting,false);
			}
		}		
	}
	
	public void pushInstance(String instanceSetting,boolean updateBindInstances) {
		if(updateBindInstances)//for recover
			this.bindInstances.offer(instanceSetting);
		String[] strs = instanceSetting.split(":");
		String[] paths = EFFileUtil.getInstancePath(strs[0]);
		this.instanceCoord.sendInstanceData(EFFileUtil.readText(paths[0], GlobalParam.ENCODING,false),
				EFFileUtil.readText(paths[1], GlobalParam.ENCODING,false),
				EFFileUtil.readText(paths[2], GlobalParam.ENCODING,false),strs[0]);
		this.instanceCoord.addInstance(instanceSetting);
		this.instanceCoord.resumeInstance(strs[0], GlobalParam.JOB_TYPE.INCREMENT.name());
		this.instanceCoord.resumeInstance(strs[0], GlobalParam.JOB_TYPE.FULL.name());
		this.masterInstanceCoorder.resumeInstance(strs[0]);
	}

	public void pushResource() {
		String resource = GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("pond");
		String instructions = GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("instructions");
		this.instanceCoord.sendData(EFFileUtil.readText(resource,GlobalParam.ENCODING,false),
				"/" + GlobalParam.StartConfig.getProperty("pond"), true);
		this.instanceCoord.sendData(EFFileUtil.readText(instructions, GlobalParam.ENCODING,false),
				"/" + GlobalParam.StartConfig.getProperty("instructions"), true);
		this.instanceCoord.reloadResource();
	}
	
	public String popInstance() {
		String instanceSetting = this.bindInstances.poll();
		this.removeInstance(instanceSetting);
		return instanceSetting;
	}
	
	public String popInstance(String instance) { 
		String instanceSetting;
		while(true) {
			instanceSetting = this.bindInstances.poll();
			if (instanceSetting.split(":")[0].equals(instance))
				break;
			this.bindInstances.offer(instanceSetting);
		}
		this.removeInstance(instanceSetting);
		return instanceSetting;
	}
	
	public void stopAllInstance() {
		while (!this.bindInstances.isEmpty()) {
			popInstance();
		}
	}
	
	public boolean runInstanceNow(String instance,String jobtype) {
		return this.instanceCoord.runInstanceNow(instance, jobtype);
	}
	
	public void stopInstance(String instance) {
		this.instanceCoord.stopInstance(instance, GlobalParam.JOB_TYPE.INCREMENT.name());
		this.instanceCoord.stopInstance(instance, GlobalParam.JOB_TYPE.FULL.name());
		this.masterInstanceCoorder.stopInstance(instance);
	}
		
	private void removeInstance(String instanceSetting) {
		if (instanceSetting != null) {
			String[] strs = instanceSetting.split(":");
			this.stopInstance(strs[0]);
			this.instanceCoord.removeInstance(strs[0], false);
		}
	}
}
