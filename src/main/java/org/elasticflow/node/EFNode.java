package org.elasticflow.node;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.flow.EFlowMonitor;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.yarn.EFRPCService;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.EFMonitorCoord;
import org.elasticflow.yarn.coord.InstanceCoord;
import org.elasticflow.yarn.coord.NodeCoord;
import org.elasticflow.yarn.coordinator.DistributeCoorder;

import com.alibaba.fastjson.JSONObject;

/**
 * Node Model It is mainly used for distributed node control
 * 
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
	private EFlowMonitor flowMonitor;

	public static EFNode getInstance(String ip, Integer nodeId) {
		EFNode n = new EFNode(ip, nodeId);
		n.lastLiveTime = Common.getNow();
		n.flowMonitor = new EFlowMonitor();
		return n;
	}

	public EFNode(String ip, Integer nodeId) {
		setIp(ip);
		setNodeId(nodeId);
	}

	public void init(DistributeCoorder masterInstanceCoorder,boolean reset) {
		this.nodeCoord = EFRPCService.getRemoteProxyObj(NodeCoord.class,
				new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT));
		this.instanceCoord = EFRPCService.getRemoteProxyObj(InstanceCoord.class,
				new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT));
		this.monitorCoord = EFRPCService.getRemoteProxyObj(EFMonitorCoord.class,
				new InetSocketAddress(ip, GlobalParam.SLAVE_SYN_PORT));
		if(reset) {
			this.instanceCoord.initNode();
			this.masterInstanceCoorder = masterInstanceCoorder;
			this.pushResource();
			this.stopAllInstance();
		}		
	}

	public JSONObject getNodeInfos() {
		JSONObject JO = new JSONObject();
		JO.put("ip", ip);
		JO.put("nodeId", nodeId);
		JO.put("bindInstances", bindInstances);
		return JO;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public boolean isLive() {
		if ((Common.getNow() - this.lastLiveTime) > GlobalParam.NODE_LIVE_TIME)
			setStatus(false);
		return isLive;
	}

	public boolean isOpenRegulate() {
		return this.flowMonitor.isOpenRegulate();
	}

	public double getResourceAbundance() {
		return this.flowMonitor.getResourceAbundance();
	}

	public void refresh() {
		this.lastLiveTime = Common.getNow();
		Resource.threadPools.execute(() -> {
			try {
				double tmp[] = nodeCoord.summaryResource();
				this.cpuUsed = tmp[0];
				this.memUsed = tmp[1];
				this.flowMonitor.checkResourceUsage();
			}catch (Exception e) {
				setStatus(false);
				Common.LOG.warn("ip {},nodeId {}, can not connect!",ip,nodeId);
			}
		});
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

	public double getMemUsed() {
		return memUsed;
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

	public boolean needRecover() {
		if (this.instanceCoord.onlineTasksNum()<1) 
			return true;
		return false;
	}
	
	public void recoverInstance() {
		for (String instanceSetting : this.bindInstances) {
			this.pushInstance(instanceSetting, false);
		}
	}

	public void pushInstance(String instanceSetting, boolean updateBindInstances) {
		if (updateBindInstances)// for recover
			this.bindInstances.offer(instanceSetting);
		String[] strs = instanceSetting.split(":");
		String[] paths = EFFileUtil.getInstancePath(strs[0]);
		this.masterInstanceCoorder.stopInstance(strs[0]);
		this.instanceCoord.sendInstanceData(EFFileUtil.readText(paths[0], GlobalParam.ENCODING, true),
				EFFileUtil.readText(paths[1], GlobalParam.ENCODING, true),
				EFFileUtil.readText(paths[2], GlobalParam.ENCODING, true), strs[0]);
		this.masterInstanceCoorder.resumeInstance(strs[0]);
		this.instanceCoord.loadInstance(instanceSetting, true,false);
	}

	public void pushResource() {
		String resource = GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("pond");
		String instructions = GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("instructions");
		this.instanceCoord.sendData(EFFileUtil.readText(resource, GlobalParam.ENCODING, false),
				"/" + GlobalParam.StartConfig.getProperty("pond"), true);
		this.instanceCoord.sendData(EFFileUtil.readText(instructions, GlobalParam.ENCODING, false),
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
		while (true) {
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

	public void resetBreaker(String instance, String L1seq) {
		this.instanceCoord.resetBreaker(instance, L1seq);
	}

	public JSONObject getBreakerStatus(String instance, String L1seq, String appendPipe) {
		return this.instanceCoord.getBreakerStatus(instance, L1seq, appendPipe);
	}

	public boolean runInstanceNow(String instance, String jobtype, boolean asyn) {
		return this.instanceCoord.runInstanceNow(instance, jobtype, asyn);
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
