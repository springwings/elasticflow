/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.node.monitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.config.GlobalParam.INSTANCE_STATUS;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.GlobalParam.TASK_FLOW_SINGAL;
import org.elasticflow.model.EFRequest;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.task.FlowStatistic;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.writer.WriterFlowSocket;
import org.elasticflow.yarn.Resource;
import org.mortbay.jetty.Request;

import com.alibaba.fastjson.JSONObject;

/**
 * Instance Manage
 * 
 * @author chengwen
 * @version 3.0
 * @create_time 2021-07-30
 */
public class InstanceManage {

	private NodeMonitor NM;

	public InstanceManage(NodeMonitor NM) {
		this.NM = NM;
		this.NM.instanceActions.putAll(new HashMap<String, String>() {
			private static final long serialVersionUID = -8313429841889556616L;
			{
				// instance manage
				put("getinstances", "getInstances");
				put("addinstance", "addInstance");
				put("cloneinstance", "cloneInstance");
				put("resetinstancestate", "resetInstanceState");
				put("getinstanceseqs", "getInstanceSeqs");
				put("reloadinstance", "reloadInstance");
				put("runnow", "runNow");
				put("resetbreaker", "resetBreaker");
				put("addinstancetosystem", "addInstanceToSystem");
				put("stopinstance", "stopInstance");
				put("resumeinstance", "resumeInstance");
				put("removeinstance", "removeInstance");
				put("deleteinstancedata", "deleteInstanceData");
				put("getinstanceinfo", "getInstanceInfo");
				put("instanceflowgraph", "instanceFlowGraph");
				put("getinstancexml", "getInstanceXml");
				put("updateinstancexml", "updateInstanceXml");
				put("searchinstancedata", "searchInstanceData");
				put("analyzeinstance", "analyzeInstance");
			}
		});
	}

	/**
	 * close instance breaker state
	 * 
	 * @param rq
	 * @param RR
	 * @throws EFException
	 */
	public void resetBreaker(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance")) {
			try {
				Boolean state = EFMonitorUtil.resetBreaker(RR.getStringParam("instance"));
				if (state) {
					this.NM.setResponse(RESPONSE_STATUS.Success, null, "");
				} else {
					this.NM.setResponse(RESPONSE_STATUS.DataErr, RR.getStringParam("instance") + " instance not exits!",
							null);
				}
			} catch (EFException e) {
				this.NM.setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
			}
		}
	}

	/**
	 * get instance detail informations.
	 * 
	 * @param rq
	 * @throws EFException
	 */
	public void getInstanceInfo(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance")) {
			try {
				JSONObject JO = EFMonitorUtil.getInstanceInfo(RR.getStringParam("instance"), 7);
				if (JO.isEmpty()) {
					this.NM.setResponse(RESPONSE_STATUS.DataErr, RR.getStringParam("instance") + " instance not exits!",
							null);
				} else {
					this.NM.setResponse(RESPONSE_STATUS.Success, null, JO);
				}
			} catch (EFException e) {
				this.NM.setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
			}
		}
	}

	/**
	 * Obtain the correlation relationship diagram between instance data streams.
	 * 
	 * @param rq
	 * @param RR
	 */
	public void instanceFlowGraph(Request rq, EFRequest RR) {
		HashMap<String, Object> result = new HashMap<String, Object>();
		Map<String, InstanceConfig> instances = Resource.nodeConfig.getInstanceConfigs();
		HashMap<String, JSONObject> graphnodes = new HashMap<String, JSONObject>();
		List<JSONObject> edges = new ArrayList<JSONObject>();

		for (Map.Entry<String, InstanceConfig> entry : instances.entrySet()) {
			InstanceConfig config = entry.getValue();
			JSONObject instance = new JSONObject();
			instance.put("Instance", entry.getKey());
			instance.put("Alias", config.getAlias());
			instance.put("OptimizeCron", config.getPipeParams().getOptimizeCron());
			instance.put("DeltaCron", config.getPipeParams().getDeltaCron());
			if (config.getPipeParams().getFullCron() == null && config.getPipeParams().getReadFrom() != null
					&& config.getPipeParams().getWriteTo() != null) {
				instance.put("FullCron", "0 0 0 1 1 ? 2099");
			} else {
				instance.put("FullCron", config.getPipeParams().getFullCron());
			}
			instance.put("SearchFrom", config.getPipeParams().getSearchFrom());
			instance.put("ReadFrom", config.getPipeParams().getReadFrom());
			switch (Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom()).getType()) {
			case KAFKA:
			case ROCKETMQ:
				instance.put("ReadFrom",
						Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom()).getHost() + "#"
								+ Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom())
										.getDefaultValue().getString("consumer.topic"));
				break;
			default:
				instance.put("ReadFrom",
						Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom()).getHost() + "#"
								+ Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom())
										.getL1seq());
				break;
			}
			List<String> wt = Arrays.asList(config.getPipeParams().getWriteTo().split(","));
			List<String> wt2 = new ArrayList<>();
			if (config.getPipeParams().getWriteTo() != null
					&& Resource.nodeConfig.getWarehouse().containsKey(config.getPipeParams().getWriteTo())) {
				switch (Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getWriteTo()).getType()) {
				case KAFKA:
				case ROCKETMQ:
					for (String _wt : wt) {
						wt2.add(Resource.nodeConfig.getWarehouse().get(_wt).getHost() + "#"
								+ config.getWriteFields().get("topic").getDefaultvalue());
					}
					break;
				default:
					for (String _wt : wt) {
						wt2.add(Resource.nodeConfig.getWarehouse().get(_wt).getHost() + "#"
								+ Resource.nodeConfig.getWarehouse().get(_wt).getL1seq());
					}
					break;
				}
			}
			instance.put("WriteTo", wt2);
			instance.put("OpenTrans", config.openTrans());
			instance.put("OpenCompute", config.openCompute());
			instance.put("IsVirtualPipe", config.getPipeParams().isVirtualPipe());
			instance.put("InstanceType", EFMonitorUtil.getInstanceType(config.getInstanceType()));
			graphnodes.put(config.getAlias(), instance);
			try {
				if (!graphnodes.get(config.getAlias()).containsKey("instance_status")) {
					JSONObject JO = EFMonitorUtil.getInstanceInfo(config.getAlias(), 8);
					graphnodes.get(config.getAlias()).put("instance_status", JO.getInteger("instance_status"));
				}
			} catch (Exception e) {
				Common.LOG.warn("getInstanceInfo exception", e);
			}
		}

		// start map resource
		for (Entry<String, JSONObject> node : graphnodes.entrySet()) {
			String readfrom = node.getValue().getString("ReadFrom");
			int weight = 0;
			try {
				JSONObject JO = EFMonitorUtil.getInstanceInfo(node.getKey(), 2);
				JSONObject _datas = JO.getJSONObject("reader");
				for (String _key : _datas.keySet()) {
					if (_datas.getJSONObject(_key).containsKey("totalProcess"))
						weight += _datas.getJSONObject(_key).getInteger("totalProcess");
				}
			} catch (Exception e) {
				node.getValue().put("instance_status", INSTANCE_STATUS.Error.getVal());
				e.printStackTrace();
			}

			for (Entry<String, JSONObject> _entry : graphnodes.entrySet()) {
				if (!node.getKey().equals(_entry.getKey())
						&& _entry.getValue().getJSONArray("WriteTo").contains(readfrom)) {
					JSONObject edge = new JSONObject();
					edge.put("from", _entry.getKey());
					edge.put("weight", weight);
					edge.put("to", node.getKey());
					if (RR.getBooleanParam("track")) {
						edge.put("isconnect", false);
						String key = RR.getStringParam("track_field");
						String val = RR.getStringParam("track_value");
						edge.put("isconnect", EFMonitorUtil.containCheck(node.getKey(), key, val));
					} else {
						edge.put("isconnect", true);
						if (node.getValue().getBoolean("OpenTrans") == false)
							edge.put("isconnect", false);
					}
					edges.add(edge);
				}
			}
		}
		// Modify node attributes:Only egress E, only ingress B,both eg/in M,
		// independent nodes S
		for (Entry<String, JSONObject> node : graphnodes.entrySet()) {
			int egress = 0;
			int ingress = 0;
			for (JSONObject edge : edges) {
				if (node.getKey().equals(edge.get("from")))
					egress += 1;
				if (node.getKey().equals(edge.get("to")))
					ingress += 1;
			}
			if (egress > 0 && ingress > 0) {
				node.getValue().put("attribute", "M");
			} else if (egress == 0 && ingress > 0) {
				node.getValue().put("attribute", "E");
			} else if (egress == 0 && ingress == 0) {
				node.getValue().put("attribute", "S");
			} else {
				node.getValue().put("attribute", "B");
			}
			node.getValue().put("weight", egress + ingress);
		}
		result.put("nodes", graphnodes);
		result.put("edges", edges);
		this.NM.setResponse(RESPONSE_STATUS.Success, null, result);
	}

	public void getInstanceXml(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance")) {
			String xmlPath = GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance") + "/task.xml";
			byte[] datas = EFDataStorer.getData(xmlPath, false);
			this.NM.setResponse(RESPONSE_STATUS.Success, "", new String(datas));
		}
	}

	/**
	 * Direct coverage, therefore the content must be complete
	 * 
	 * @param rq
	 * @param RR
	 */
	public void updateInstanceXml(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance,content")) {
			String xmlPath = GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance") + "/task.xml";
			EFDataStorer.setData(xmlPath, new String(NodeMonitor.decoder.decode(RR.getStringParam("content"))));
			this.NM.setResponse(RESPONSE_STATUS.Success, "update " + RR.getStringParam("instance") + " success", "");
		}
	}

	/**
	 * search instance datas.
	 * 
	 * @param rq
	 * @throws EFException
	 */
	public void searchInstanceData(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance")) {
			EFResponse rps = EFResponse.getInstance();
			String pipe = RR.getStringParam("instance");
			Map<String, InstanceConfig> configMap = Resource.nodeConfig.getSearchConfigs();
			EFRequest efRq = Common.getEFRequest(rq, rps);
			if (configMap.containsKey(pipe))
				Resource.socketCenter.getSearcher(pipe, "", "", false).startSearch(efRq, rps);
			this.NM.setResponse(RESPONSE_STATUS.Success, null, rps);
		}
	}

	public void analyzeInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance")) {
			String res = "";
			if (GlobalParam.DISTRIBUTE_RUN) {
				res = GlobalParam.INSTANCE_COORDER.distributeCoorder().analyzeInstance(RR.getStringParam("instance"));
			}else {
				res = EFMonitorUtil.analyzeInstance(RR.getStringParam("instance"));
			}  
			this.NM.setResponse(RESPONSE_STATUS.Success, null, res);
		}
	}

	/**
	 * get all instances info
	 * 
	 * @param rq
	 */
	public void getInstances(Request rq, EFRequest RR) {
		Map<String, InstanceConfig> nodes = Resource.nodeConfig.getInstanceConfigs();
		HashMap<String, List<JSONObject>> rs = new HashMap<String, List<JSONObject>>();
		JSONObject instance_node = null;
		if (GlobalParam.DISTRIBUTE_RUN)
			instance_node = GlobalParam.INSTANCE_COORDER.distributeCoorder().getInstanceNode();
		for (Map.Entry<String, InstanceConfig> entry : nodes.entrySet()) {
			InstanceConfig config = entry.getValue();
			JSONObject instance = new JSONObject();
			instance.put("Instance", entry.getKey());
			instance.put("QueryApi", "//" + GlobalParam.IP + ":" + GlobalParam.SystemConfig.get("searcher_service_port")
					+ "/" + entry.getKey());
			instance.put("Alias", config.getAlias());
			instance.put("OptimizeCron", config.getPipeParams().getOptimizeCron());
			if (config.getPipeParams().getDeltaCron() == null) {
				instance.put("DeltaCron", "未配置");
			}else {
				instance.put("DeltaCron", config.getPipeParams().getDeltaCron());
			} 
			if (config.getPipeParams().getFullCron() == null) {
				instance.put("FullCron", "未配置");
			} else {
				instance.put("FullCron", config.getPipeParams().getFullCron());
			}
			if (GlobalParam.DISTRIBUTE_RUN) {
				instance.put("Nodes", instance_node.getJSONArray(entry.getKey()));
			} else {
				instance.put("Nodes", Arrays.asList(GlobalParam.IP));
			}
			instance.put("SearchFrom", config.getPipeParams().getSearchFrom());
			instance.put("ReadFrom", config.getPipeParams().getReadFrom());
			instance.put("WriteTo", config.getPipeParams().getWriteTo().replace(",", ";"));
			instance.put("OpenTrans", config.openTrans());
			instance.put("OpenCompute", config.openCompute());
			instance.put("RunState", true);
			instance.put("Remark", config.getPipeParams().getRemark());
			try {
				if (config.openTrans()) {
					String[] L1seqs = TaskUtil.getL1seqs(config);
					for (String L1seq : L1seqs) {
						JSONObject tmp = GlobalParam.INSTANCE_COORDER.distributeCoorder()
								.getBreakerStatus(config.getInstanceID(), L1seq, "");
						if (tmp.getBoolean("breaker_is_on"))
							instance.put("RunState", false);
					}
				} else {
					instance.put("RunState", true);
				}
			} catch (Exception e) {
				Common.LOG.warn("TaskUtil.getL1seqs exception", e);
			}
			instance.put("IsVirtualPipe", config.getPipeParams().isVirtualPipe());
			instance.put("InstanceType", EFMonitorUtil.getInstanceType(config.getInstanceType()));

			if (rs.containsKey(config.getAlias())) {
				rs.get(config.getAlias()).add(instance);
				rs.put(config.getAlias(), rs.get(config.getAlias()));
			} else {
				ArrayList<JSONObject> tmp = new ArrayList<JSONObject>();
				tmp.add(instance);
				rs.put(config.getAlias(), tmp);
			}
		}
		this.NM.setResponse(RESPONSE_STATUS.Success, null, rs);
	}

	/**
	 * Perform the instance task immediately
	 * 
	 * @param rq
	 */
	public void runNow(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance,jobtype")) {
			if (Resource.nodeConfig.getInstanceConfigs().containsKey(RR.getStringParam("instance"))
					&& Resource.nodeConfig.getInstanceConfigs().get(RR.getStringParam("instance")).openTrans()) {
				boolean state;
				if (GlobalParam.DISTRIBUTE_RUN) {
					state = GlobalParam.INSTANCE_COORDER.distributeCoorder()
							.runClusterInstanceNow(RR.getStringParam("instance"), RR.getStringParam("jobtype"), true);
				} else {
					state = Resource.flowCenter.runInstanceNow(RR.getStringParam("instance"),
							RR.getStringParam("jobtype"), true);
				}

				if (state) {
					this.NM.setResponse(RESPONSE_STATUS.Success,
							"Writer " + RR.getStringParam("instance") + " job has been started now!", null);
				} else {
					this.NM.setResponse(RESPONSE_STATUS.DataErr, "Writer " + RR.getStringParam("instance")
							+ " job not exists or run failed or had been stated!", null);
				}
			} else {
				this.NM.setResponse(RESPONSE_STATUS.DataErr,
						"Writer " + RR.getStringParam("instance") + " job not open in this node!Run start faild!",
						null);
			}
		}
	}

	/**
	 * Only delete tasks from the configuration file and keep data
	 * 
	 * @param rq
	 * @param RR
	 */
	public void removeInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance")) {
			removeInstance(RR.getStringParam("instance"));
			this.NM.setResponse(RESPONSE_STATUS.Success,
					"Writer " + RR.getStringParam("instance") + " job have removed!", null);
		}
	}

	/**
	 * remove instance stop all jobs and remove from configure file.
	 * 
	 * @param instance
	 */
	public void removeInstance(String instance) {
		if (GlobalParam.DISTRIBUTE_RUN) {
			GlobalParam.INSTANCE_COORDER.distributeCoorder().removeInstanceFromCluster(instance, false);
			GlobalParam.INSTANCE_COORDER.removeInstance(instance, false);
		} else {
			GlobalParam.INSTANCE_COORDER.removeInstance(instance, true);
		}
		try {
			EFMonitorUtil.removeConfigInstance(instance);
			EFMonitorUtil.saveNodeConfig();
		} catch (Exception e) {
			this.NM.setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
		}
	}

	/**
	 * stop instance job.
	 * 
	 * @param rq
	 */
	public void stopInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance,type")) {
			GlobalParam.INSTANCE_COORDER.stopInstance(RR.getStringParam("instance"), RR.getStringParam("type"));
			this.NM.setResponse(RESPONSE_STATUS.Success,
					"Writer " + RR.getStringParam("instance") + " job stopped successfully!", null);
		}
	}

	/**
	 * resume instance job.
	 * 
	 * @param rq
	 */
	public void resumeInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance,type")) {
			GlobalParam.INSTANCE_COORDER.resumeInstance(RR.getStringParam("instance"), RR.getStringParam("type"));
			this.NM.setResponse(RESPONSE_STATUS.Success,
					"Writer " + RR.getStringParam("instance") + " job resumed successfully!", null);
		}
	}

	/**
	 * add instance setting into system and add it to configure file also.
	 * 
	 * @param rq
	 * @param RR
	 */
	public void addInstanceToSystem(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance,level")) {
			boolean successLoad = EFMonitorUtil.addInstanceToSystem(RR.getStringParam("instance"), RR.getStringParam("level"));
			try {
				if(successLoad) {
					EFMonitorUtil.saveNodeConfig();
					this.NM.setResponse(RESPONSE_STATUS.Success,
							RR.getStringParam("instance") + " add to node " + GlobalParam.IP + " success!", null);
				}else {
					this.NM.setResponse(RESPONSE_STATUS.CodeException, "failed add instance!", null);
				}				
			} catch (Exception e) {
				this.NM.setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
			}
		}
	}

	/**
	 * delete Instance Data through alias or Instance data name The time mechanism
	 * deletes the current time index The A/B mechanism deletes the currently used
	 * index
	 * 
	 * @param alias
	 * @return
	 */
	public void deleteInstanceData(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance")) {
			String _instance = RR.getStringParam("instance");
			Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
			boolean state = true;
			for (Map.Entry<String, InstanceConfig> ents : configMap.entrySet()) {
				String instance = ents.getKey();
				InstanceConfig instanceConfig = ents.getValue();
				if (instance.equals(_instance) || instanceConfig.getAlias().equals(_instance)) {
					String[] L1seqs = EFMonitorUtil.getInstanceL1seqs(instance);
					if (L1seqs.length == 0) {
						L1seqs = new String[1];
						L1seqs[0] = GlobalParam.DEFAULT_RESOURCE_SEQ;
					}
					EFMonitorUtil.controlInstanceState(instance, TASK_FLOW_SINGAL.Stop, true);
					for (String L1seq : L1seqs) {
						String tags = TaskUtil.getResourceTag(instance, L1seq, GlobalParam.FLOW_TAG._DEFAULT.name(),
								false);
						try {
							WriterFlowSocket wfs = Resource.socketCenter.getWriterSocket(
									Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getWriteTo(),
									instance, L1seq, tags);
							wfs.PREPARE(false, false);
							if (wfs.connStatus()) {
								wfs.removeShard(instance,
										GlobalParam.TASK_COORDER.getStoreIdFromSave(instance, L1seq, true, false));
								wfs.releaseConn(false, true);
							}
						} catch (EFException e) {
							state = false;
							Common.LOG.error("delete {} Instance Data", instance, e);
						}
					}
					EFMonitorUtil.controlInstanceState(instance, TASK_FLOW_SINGAL.Ready, true);
				}
			}
			if (state) {
				this.NM.setResponse(RESPONSE_STATUS.Success, "delete " + _instance + " success!", null);
			} else {
				this.NM.setResponse(RESPONSE_STATUS.CodeException, "delete " + _instance + " failed!", null);
			}
		}
	}

	/**
	 * Rebuilding task instances through configuration
	 * 
	 * @param rq instance=xx&reset=true|false&runtype=1 reset is true will clear all
	 *           instances, instance settings. runType=-1 Use the original task run
	 *           type
	 * 
	 * @throws EFException
	 * 
	 */
	public void reloadInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance,runtype,reset")) {
			String instance = RR.getStringParam("instance");
			String reset = RR.getStringParam("reset");
			String runType = RR.getStringParam("runtype");
			if (!Resource.nodeConfig.getInstanceConfigs().containsKey(instance)) {
				this.NM.setResponse(RESPONSE_STATUS.DataErr, instance + " not exists!", null);
			} else {
				try {
					if (runType.equals("-1"))
						runType = String
								.valueOf(Resource.nodeConfig.getInstanceConfigs().get(instance).getInstanceType());
					EFFileUtil.deleteFile(EFFileUtil.getInstancePath(instance)[0]);
					EFFileUtil.deleteFile(EFFileUtil.getInstancePath(instance)[1]);
					EFFileUtil.deleteFile(EFFileUtil.getInstancePath(instance)[3]);
					EFMonitorUtil.reloadInstance(instance, reset, runType);
					this.NM.setResponse(RESPONSE_STATUS.Success,
							RR.getStringParam("instance") + " reload instance settings success!", null);
				} catch (EFException e) {
					this.NM.setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
				}
			}
		}
	}

	/**
	 * reset Instance full and increment running state
	 * 
	 * @param rq
	 */
	public void resetInstanceState(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance")) {
			try {
				String instance = RR.getStringParam("instance");
				String val = "0";
				if (RR.getParams().get("set_value") != null)
					val = RR.getStringParam("set_value");
				String[] L1seqs = EFMonitorUtil.getInstanceL1seqs(instance);
				for (String L1seq : L1seqs) {
					GlobalParam.TASK_COORDER.batchUpdateSeqPos(instance, val, false);
					GlobalParam.TASK_COORDER.saveTaskInfo(instance, L1seq,
							GlobalParam.TASK_COORDER.getStoreIdFromSave(instance, L1seq, false, false), false);
					if (GlobalParam.DISTRIBUTE_RUN) {
						GlobalParam.INSTANCE_COORDER.distributeCoorder().resetPipeEndStatus(instance, L1seq);
					} else {
						EFMonitorUtil.resetPipeEndStatus(instance, L1seq);
					}
					// update flow status,Distributed environment synchronization status
					if (GlobalParam.DISTRIBUTE_RUN) {
						Resource.flowStates.get(instance).put(FlowStatistic.getStoreKey(L1seq),
								GlobalParam.INSTANCE_COORDER.distributeCoorder().getPipeEndStatus(instance, L1seq));
					} else {
						Resource.flowStates.get(instance).put(FlowStatistic.getStoreKey(L1seq),
								EFMonitorUtil.getPipeEndStatus(instance, L1seq));
					}
				}
				EFFileUtil.createAndSave(Resource.flowStates.get(instance).toJSONString(),
						EFFileUtil.getInstancePath(instance)[3]);
				this.NM.setResponse(RESPONSE_STATUS.Success, RR.getStringParam("instance") + " reset Success!", null);
			} catch (Exception e) {
				this.NM.setResponse(RESPONSE_STATUS.DataErr, RR.getStringParam("instance") + " not exists!", null);
			}
		}
	}

	public void cloneInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance,new_instance_name")) {
			EFFileUtil.copyFolder(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance"),
					GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("new_instance_name"));
			EFFileUtil.delFile(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("new_instance_name") + "/"
					+ GlobalParam.JOB_FULLINFO_PATH);
			EFFileUtil.delFile(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("new_instance_name") + "/"
					+ GlobalParam.JOB_INCREMENTINFO_PATH);
			this.NM.setResponse(RESPONSE_STATUS.Success, RR.getStringParam("new_instance_name") + " clone success!",
					null);
		}
	}

	/**
	 * push instance to system
	 * 
	 * @param rq
	 * @param RR
	 */
	public void addInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance,content,level")) {
			String xmlPath = GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance") + "/task.xml";
			try {
				EFDataStorer.createPath(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance"), false);
				EFDataStorer.setData(xmlPath, new String(NodeMonitor.decoder.decode(RR.getStringParam("content"))));
				EFMonitorUtil.addInstanceToSystem(RR.getStringParam("instance"), RR.getStringParam("level"));
				EFMonitorUtil.saveNodeConfig();
				this.NM.setResponse(RESPONSE_STATUS.Success,
						RR.getStringParam("instance") + " save and push to node " + GlobalParam.IP + " success!", null);
			} catch (Exception e) {
				this.NM.setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
			}
		}
	}

	/**
	 * Data source level delimited sequence
	 * 
	 * @param rq
	 * @param RR
	 */
	public void getInstanceSeqs(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this.NM, RR, "instance")) {
			try {
				String instance = RR.getStringParam("instance");
				InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instance);
				WarehouseParam dataMap = Resource.nodeConfig.getWarehouse()
						.get(instanceConfig.getPipeParams().getReadFrom());
				this.NM.setResponse(RESPONSE_STATUS.Success, null, StringUtils.join(dataMap.getL1seq(), ","));
			} catch (Exception e) {
				this.NM.setResponse(RESPONSE_STATUS.CodeException, RR.getStringParam("instance") + " not exists!",
						null);
			}
		}
	}
}
