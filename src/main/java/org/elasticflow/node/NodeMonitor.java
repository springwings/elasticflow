/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.node;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.config.GlobalParam.INSTANCE_STATUS;
import org.elasticflow.config.GlobalParam.RESOURCE_TYPE;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.GlobalParam.TASK_FLOW_SINGAL;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.model.EFRequest;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.InstructionTree;
import org.elasticflow.model.task.FlowStatistic;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFLoader;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.PipeXMLUtil;
import org.elasticflow.util.SystemInfoUtil;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.writer.WriterFlowSocket;
import org.elasticflow.yarn.Resource;
import org.mortbay.jetty.Request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * * data-flow router maintain apis,default port
 * 8617,localhost:8617/ef.doaction?ac=[actions]
 * 
 * @actions reloadConfig reload instance config and re-add all jobs clean relate
 *          pools
 * @actions runNow/stopInstance/removeInstance/resumeInstance
 *          start/stop/remove/resume once now instance
 * @actions getInstances get all instances in current node
 * @actions getInstanceInfo get specify instance detail informations
 * @author chengwen
 * @version 3.0
 * @date 2018-10-25 09:08
 */
@NotThreadSafe
public final class NodeMonitor {

	private RESPONSE_STATUS response_status = RESPONSE_STATUS.CodeException;

	private String response_info;

	private Object response_data;

	static Decoder decoder = Base64.getDecoder();

	private HashMap<String, String> actions = new HashMap<String, String>() {
		private static final long serialVersionUID = -8313429841889556616L;
		{
			// node manage
			put("getnodeconfig", "getNodeConfig");
			put("setnodeconfig", "setNodeConfig");
			put("getnodeconfigcontent", "getNodeConfigContent");
			put("updatenodeconfigcontent", "updateNodeConfigContent");
			put("globalstatus", "globalStatus");
			put("getstatus", "getStatus");
			put("startsearcherservice", "startSearcherService");
			put("stopsearcherservice", "stopSearcherService");
			put("starthttpreaderserviceservice", "startHttpReaderServiceService");
			put("stopHttpreaderserviceservice", "stopHttpReaderServiceService");
			put("restartnode", "restartNode");
			put("stopnode", "stopNode");
			put("restartcluster", "restartCluster");
			put("stopcluster", "stopCluster");
			put("loadhandler", "loadHandler");
			put("runcode", "runCode");
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
			// other manage 
			put("getresource", "getResource");
			put("getresources", "getResources");
			put("getresourcexml", "getResourcexml");
			put("updateresource", "updateResource");
			put("addresource", "addResource");
			put("removeresource", "removeResource");
			put("setinstancepipeconfig", "setInstancePipeConfig");
		}
	};

	/**
	 * 
	 * @param status 0 faild 1 success
	 * @param info   response information
	 */
	public void setResponse(RESPONSE_STATUS status, String info, Object data) {
		this.response_status = status;
		this.response_info = info;
		this.response_data = data;
	}

	public void ac(Request rq, EFRequest RR, EFResponse RS) {
		try {
			if (RR.getParams().get("ac") != null && this.actions.containsKey(RR.getStringParam("ac").toLowerCase())) {
				Method m = NodeMonitor.class.getMethod(this.actions.get(RR.getStringParam("ac").toLowerCase()),
						Request.class, EFRequest.class);
				m.invoke(this, rq, RR);
				RS.setStatus(this.response_info, this.response_status);
				RS.setPayload(this.response_data);
			} else {
				RS.setPayload(this.actions);
				RS.setStatus("Example: /efm.doaction?ac=getStatus", RESPONSE_STATUS.ParameterErr);
			}
		} catch (Exception e) {
			RS.setStatus("Actions Exception!", RESPONSE_STATUS.CodeException);
			Common.LOG.error("Management Operations {} exception", RR.getParams().get("ac"), e);
		}
	}

	/*-----------------------// node manage------------------ */

	/**
	 * Be care full,this will remove all relative instance
	 * 
	 * @param rq
	 */
	public void removeResource(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "name,type")) {
			String name = RR.getStringParam("name");
			RESOURCE_TYPE type = RESOURCE_TYPE.valueOf(RR.getStringParam("type").toUpperCase());
			String[] seqs;
			WarehouseParam wp;

			Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
			for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) {
				InstanceConfig instanceConfig = entry.getValue();
				if (instanceConfig.getPipeParams().getReadFrom().equals(name)) {
					removeInstance(entry.getKey());
				}
				if (instanceConfig.getPipeParams().getWriteTo().equals(name)) {
					removeInstance(entry.getKey());
				}
				if (instanceConfig.getPipeParams().getSearchFrom().equals(name)) {
					removeInstance(entry.getKey());
				}
			}

			switch (type) {
			case WAREHOUSE:
				wp = Resource.nodeConfig.getWarehouse().get(name);
				seqs = wp.getL1seq();
				if (seqs.length > 0) {
					for (String seq : seqs) {
						EFConnectionPool.clearPool(wp.getPoolName(seq));
					}
				} else {
					EFConnectionPool.clearPool(wp.getPoolName(null));
				}
				break;

			case INSTRUCTION:
				Resource.nodeConfig.getInstructions().remove(name);
				break;
			}
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("name", name);
			updateResourceXml(jsonObject, true);
		}
	}
	
	/**
	 * read Resources
	 * @param rq
	 * @param RR
	 */
	public void getResources(Request rq, EFRequest RR) {
		JSONObject res = new JSONObject();
		Map<String, WarehouseParam> resources = Resource.nodeConfig.getWarehouse();
		for (Map.Entry<String, WarehouseParam> entry : resources.entrySet()) {
			WarehouseParam wp = entry.getValue();
			res.put(entry.getKey(), new JSONObject());
			res.getJSONObject(entry.getKey()).put("name", entry.getKey());
			res.getJSONObject(entry.getKey()).put("hosts", wp.getHost());
			res.getJSONObject(entry.getKey()).put("type", wp.getType());
			JSONObject pools = new JSONObject();
			for (String seq : wp.getL1seq()) {
				pools.put((seq == "" ? "DEFAULT" : seq), EFMonitorUtil.getConnectionStatus(wp.getPoolName(seq)));
			}
			res.getJSONObject(entry.getKey()).put("pools", pools);
			String[] hosts = wp.getHost().split(",");
			if (EFMonitorUtil.isPortOpen(hosts[0])) {
				res.getJSONObject(entry.getKey()).put("status",
						Resource.resourceStates.get(entry.getKey()).getString("status"));
			} else {
				res.getJSONObject(entry.getKey()).put("status", GlobalParam.RESOURCE_STATUS.Error.name());
			}
		}
		setResponse(RESPONSE_STATUS.Success, "", res);
	}
	
	public void getResource(Request rq, EFRequest RR) { 
		if (EFMonitorUtil.checkParams(this, RR, "name")) {
			JSONObject res = new JSONObject();
			Map<String, WarehouseParam> resources = Resource.nodeConfig.getWarehouse();
			for (Map.Entry<String, WarehouseParam> entry : resources.entrySet()) { 
				if(RR.getStringParam("name").equals(entry.getKey())) {
					WarehouseParam wp = entry.getValue(); 
					res.put(entry.getKey(), new JSONObject());
					res.getJSONObject(entry.getKey()).put("name", entry.getKey());
					res.getJSONObject(entry.getKey()).put("hosts", wp.getHost());
					res.getJSONObject(entry.getKey()).put("type", wp.getType());
					res.getJSONObject(entry.getKey()).put("defaultValue", wp.getDefaultValue());
					res.getJSONObject(entry.getKey()).put("user", wp.getUser());
					res.getJSONObject(entry.getKey()).put("maxPoolSize", wp.getMaxPoolSize());
					res.getJSONObject(entry.getKey()).put("customParams", wp.getCustomParams());
					JSONObject pools = new JSONObject();
					for (String seq : wp.getL1seq()) {
						pools.put((seq == "" ? "DEFAULT" : seq), EFMonitorUtil.getConnectionStatus(wp.getPoolName(seq)));
					}
					res.getJSONObject(entry.getKey()).put("pools", pools);
					String[] hosts = wp.getHost().split(",");
					if (EFMonitorUtil.isPortOpen(hosts[0])) {
						res.getJSONObject(entry.getKey()).put("status",
								Resource.resourceStates.get(entry.getKey()).getString("status"));
					} else {
						res.getJSONObject(entry.getKey()).put("status", GlobalParam.RESOURCE_STATUS.Error.name());
					}
					break;
				}
				
			}
			setResponse(RESPONSE_STATUS.Success, "", res);
		} 
	}

	public void getResourcexml(Request rq, EFRequest RR) {
		String pondPath = GlobalParam.DATAS_CONFIG_PATH + "/" + GlobalParam.SystemConfig.getProperty("pond");
		byte[] resourceXml = EFDataStorer.getData(pondPath, false);
		setResponse(RESPONSE_STATUS.Success, "", new String(resourceXml));
	}

	public void updateResource(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "content")) {
			String pondPath = GlobalParam.DATAS_CONFIG_PATH + "/" + GlobalParam.SystemConfig.getProperty("pond");
			EFDataStorer.setData(pondPath, new String(decoder.decode(RR.getStringParam("content"))));
			setResponse(RESPONSE_STATUS.Success, "update resource!", null);
		}
	}

	/**
	 * @param socket resource configs json string
	 */
	public void addResource(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "socket")) {
			JSONObject jsonObject = JSON.parseObject(RR.getStringParam("socket"));
			Object o = null;
			Set<String> iter = jsonObject.keySet();
			try {
				o = new WarehouseParam();
				for (String key : iter) {
					Common.setConfigObj(o, WarehouseParam.class, key, jsonObject.getString(key));
				}
//				o = new InstructionParam();
//				for (String key : iter) {
//					Common.setConfigObj(o, InstructionParam.class, key, jsonObject.getString(key));
//				}
				if (o != null) {
					Resource.nodeConfig.addSource(RESOURCE_TYPE.WAREHOUSE, o);
					setResponse(RESPONSE_STATUS.Success, "add Resource to node success!", null);
					updateResourceXml(jsonObject, false);
				}
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, "add Resource to node Exception " + e.getMessage(), null);
			}
		}
	}

	/**
	 * get node start run configure parameters.
	 * 
	 * @param rq
	 */
	public void getNodeConfig(Request rq, EFRequest RR) {
		setResponse(RESPONSE_STATUS.Success, "", GlobalParam.SystemConfig);
	}

	public void getNodeConfigContent(Request rq, EFRequest RR) {
		byte[] configs = EFDataStorer.getData(GlobalParam.SYS_CONFIG_PATH + "/config.properties", false);
		setResponse(RESPONSE_STATUS.Success, "", new String(configs));
	}

	public void updateNodeConfigContent(Request rq, EFRequest RR) {
		try {
			String configPath = GlobalParam.SYS_CONFIG_PATH + "/config.properties";
			EFDataStorer.setData(configPath, new String(decoder.decode(RR.getStringParam("content"))));
			Common.loadProperties(configPath);
			setResponse(RESPONSE_STATUS.Success, "Config set success!", null);
		} catch (Exception e) {
			setResponse(RESPONSE_STATUS.CodeException, "Config save Exception " + e.getMessage(), null);
		}
	}

	/**
	 * set node start configure parameters,will auto write into file.
	 * 
	 * @param k    property key
	 * @param v    property value
	 * @param type action type,set/remove
	 */
	public void setNodeConfig(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "content")) {
			try {
				String fpath = GlobalParam.SYS_CONFIG_PATH.replace("file:", "") + "/config.properties";
				EFDataStorer.setData(fpath, RR.getStringParam("content").strip());
				Common.loadGlobalConfig(fpath);
				setResponse(RESPONSE_STATUS.Success, "Config set success!", null);
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, "Config save Exception " + e.getMessage(), null);
			}
		}
	}

	/**
	 * restart node
	 * 
	 * @param rq
	 */
	public void restartNode(Request rq, EFRequest RR) {
		if (RR.getStringParam("node_id") != null) {
			int nodeId = Integer.parseInt(RR.getStringParam("node_id"));
			if (nodeId == GlobalParam.NODEID) {
				EFMonitorUtil.restartSystem();
			} else {
				GlobalParam.INSTANCE_COORDER.distributeCoorder().restartNode(nodeId);
			}
			setResponse(RESPONSE_STATUS.Success, null, null);
		} else {
			setResponse(RESPONSE_STATUS.ParameterErr, "parameters node_id not exists!", null);
		}
	}

	/**
	 * stop node
	 * 
	 * @param rq
	 */
	public void stopNode(Request rq, EFRequest RR) {
		if (RR.getStringParam("node_id") != null) {
			int nodeId = Integer.parseInt(RR.getStringParam("node_id"));
			if (nodeId != GlobalParam.NODEID)
				GlobalParam.INSTANCE_COORDER.distributeCoorder().stopNode(nodeId);
			setResponse(RESPONSE_STATUS.Success, null, null);
		} else {
			setResponse(RESPONSE_STATUS.ParameterErr, "parameters node_id not exists!", null);
		}
	}

	/**
	 * restart Cluster
	 * 
	 * @param rq
	 */
	public void restartCluster(Request rq, EFRequest RR) {
		if (GlobalParam.DISTRIBUTE_RUN) {
			GlobalParam.INSTANCE_COORDER.distributeCoorder().restartCluster();
		} else {
			EFMonitorUtil.restartSystem();
		}
		setResponse(RESPONSE_STATUS.Success, null, null);
	}

	/**
	 * stop Cluster
	 * 
	 * @param rq
	 */
	public void stopCluster(Request rq, EFRequest RR) {
		if (GlobalParam.DISTRIBUTE_RUN) {
			GlobalParam.INSTANCE_COORDER.distributeCoorder().stopSlaves(true);
		}
		Common.stopSystem(false);
		setResponse(RESPONSE_STATUS.Success, null, null);
	}

	/**
	 * Loading Java handler classes in real time only support no dependency handler
	 * like org.elasticflow.writerUnit.handler org.elasticflow.reader.handler
	 * org.elasticflow.searcher.handler
	 * 
	 * @param rq
	 */
	public void loadHandler(Request rq, EFRequest RR) {
		if (RR.getStringParam("path") != null && RR.getStringParam("name") != null) {
			try {
				new EFLoader(RR.getStringParam("path")).loadClass(RR.getStringParam("name"));
				setResponse(RESPONSE_STATUS.Success, "Load Handler success!", null);
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, "Load Handler Exception " + e.getMessage(), null);
			}
		} else {
			setResponse(RESPONSE_STATUS.CodeException, "Parameters path not exists!", null);
		}

	}

	/**
	 * stop node http reader pipe service
	 * 
	 * @param rq
	 */
	public void stopHttpReaderServiceService(Request rq, EFRequest RR) {
		int service_level = Integer.parseInt(GlobalParam.SystemConfig.get("service_level").toString());
		if ((service_level & 4) > 0) {
			service_level -= 4;
		}
		if (Resource.httpReaderService.close()) {
			setResponse(RESPONSE_STATUS.Success, "Stop Http Reader Service Successed!", null);
		} else {
			setResponse(RESPONSE_STATUS.CodeException, "Stop Http Reader Service Failed!", null);
		}
	}

	/**
	 * start node http reader pipe service
	 * 
	 * @param rq
	 */
	public void startHttpReaderServiceService(Request rq, EFRequest RR) {
		int service_level = Integer.parseInt(GlobalParam.SystemConfig.get("service_level").toString());
		if ((service_level & 4) == 0) {
			service_level += 4;
			Resource.httpReaderService.start();
		}
		setResponse(RESPONSE_STATUS.Success, "Start Http Reader Service Successed!", null);
	}

	/**
	 * stop node searcher service
	 * 
	 * @param rq
	 */
	public void stopSearcherService(Request rq, EFRequest RR) {
		int service_level = Integer.parseInt(GlobalParam.SystemConfig.get("service_level").toString());
		if ((service_level & 1) > 0) {
			service_level -= 1;
		}
		if (Resource.searcherService.close()) {
			setResponse(RESPONSE_STATUS.Success, "Stop Searcher Service Successed!", null);
		} else {
			setResponse(RESPONSE_STATUS.CodeException, "Stop Searcher Service Failed!", null);
		}
	}

	/**
	 * open node searcher service
	 * 
	 * @param rq
	 */
	public void startSearcherService(Request rq, EFRequest RR) {
		int service_level = Integer.parseInt(GlobalParam.SystemConfig.get("service_level").toString());
		if ((service_level & 1) == 0) {
			service_level += 1;
			Resource.searcherService.start();
		}
		setResponse(RESPONSE_STATUS.Success, "Start Searcher Service Successed!", null);
	}

	/**
	 * Cluster Global Statistics
	 * 
	 * @param rq
	 * @param RR
	 */
	public void globalStatus(Request rq, EFRequest RR) {
		JSONObject res = new JSONObject();
		res.put("cluster_mode", GlobalParam.DISTRIBUTE_RUN ? "集群模式" : "单机模式");
		res.put("task_num", Resource.tasks.size());
		res.put("resource_num", Resource.nodeConfig.getWarehouse().size());
		if (GlobalParam.DISTRIBUTE_RUN) {
			res.put("node_num", GlobalParam.INSTANCE_COORDER.distributeCoorder().getNodes().size() + 1);
			HashMap<String, Object> nodeinfos = GlobalParam.INSTANCE_COORDER.distributeCoorder().getNodeStatus();
			double mem_use = SystemInfoUtil.getMemUsage();
			double total_mem = SystemInfoUtil.getMemTotal();
			double cpu_use = SystemInfoUtil.getCpuUsage();
			for (Map.Entry<String, Object> entry : nodeinfos.entrySet()) {
				@SuppressWarnings("unchecked")
				HashMap<String, Object> datas = (HashMap<String, Object>) entry.getValue();
				mem_use += Double.parseDouble(datas.get("MEMORY_USAGE").toString());
				cpu_use += Double.parseDouble(datas.get("CPU_USAGE").toString());
				total_mem += Double.parseDouble(datas.get("MEMORY").toString());
			}
			res.put("memory_usage",
					mem_use / (GlobalParam.INSTANCE_COORDER.distributeCoorder().getNodes().size() + 1.));
			res.put("cpu_usage", cpu_use / (GlobalParam.INSTANCE_COORDER.distributeCoorder().getNodes().size() + 1.));
			res.put("total_memory", total_mem);
		} else {
			res.put("node_num", 1);
			res.put("total_memory", SystemInfoUtil.getMemTotal());
			res.put("memory_usage", SystemInfoUtil.getMemUsage());
			res.put("cpu_usage", SystemInfoUtil.getCpuUsage());
		}
		res.put("version", GlobalParam.VERSION);
		boolean health = true;
		for (Map.Entry<String, WarehouseParam> entry : Resource.nodeConfig.getWarehouse().entrySet()) {
			WarehouseParam wp = entry.getValue();
			String[] hosts = wp.getHost().split(",");
			if (!EFMonitorUtil.isPortOpen(hosts[0])) {
				health = false;
			}
		}
		res.put("is_debug", GlobalParam.DEBUG);
		res.put("lang", GlobalParam.LANG);
		res.put("run_env", GlobalParam.RUN_ENV);
		res.put("write_batch", GlobalParam.WRITE_BATCH);
		res.put("proxy_ip", GlobalParam.PROXY_IP);
		res.put("sys_config_path", GlobalParam.SYS_CONFIG_PATH);
		res.put("datas_config_path", GlobalParam.DATAS_CONFIG_PATH);
		res.put("plugin_path", GlobalParam.pluginPath);
		res.put("health", health ? "正常" : "异常");
		res.put("error_ignore", Resource.getErrorStates(ELEVEL.Ignore));
		res.put("error_dispose", Resource.getErrorStates(ELEVEL.Dispose));
		res.put("error_breakoff", Resource.getErrorStates(ELEVEL.BreakOff));
		res.put("error_termination", Resource.getErrorStates(ELEVEL.Termination));
		res.put("system_start_time", Common.FormatTime(GlobalParam.SYS_START_TIME));
		// reader computer writer data statistics
		try {
			JSONObject reader = null;
			JSONObject computer = null;
			JSONObject writer = null;
			for (Map.Entry<String, InstanceConfig> entry : Resource.nodeConfig.getInstanceConfigs().entrySet()) {
				if (!entry.getValue().openTrans())
					continue;
				JSONObject JO = EFMonitorUtil.getInstanceInfo(entry.getKey(), 2);
				if (!JO.isEmpty()) {
					if (reader == null && JO.getJSONObject("reader").containsKey("flow_state")) {
						reader = (JSONObject) JO.getJSONObject("reader").getJSONObject("flow_state").getJSONObject("historyProcess").clone();
					} else if (reader != null && JO.getJSONObject("reader").containsKey("flow_state")) {
						JSONObject _reader = JO.getJSONObject("reader").getJSONObject("flow_state")
								.getJSONObject("historyProcess");
						for (String key : reader.keySet()) {
							if (_reader.containsKey(key)) {
								reader.put(key, reader.getLongValue(key) + _reader.getLongValue(key));
							} else {
								reader.put(key, _reader.getLongValue(key));
							}
						}
					}

					if (writer == null && JO.getJSONObject("writer").containsKey("flow_state"))
						writer = (JSONObject) JO.getJSONObject("writer").getJSONObject("flow_state").getJSONObject("historyProcess").clone();
					else if (writer != null && JO.getJSONObject("writer").containsKey("flow_state")) {
						JSONObject _writer = JO.getJSONObject("writer").getJSONObject("flow_state")
								.getJSONObject("historyProcess");
						for (String key : writer.keySet()) {
							if (_writer.containsKey(key)) {
								writer.put(key, writer.getLongValue(key) + _writer.getLongValue(key));
							} else {
								writer.put(key, _writer.getLongValue(key));
							}
						}
					}

					if (computer == null && JO.getJSONObject("computer").containsKey("flow_state")) {
						computer = (JSONObject) JO.getJSONObject("computer").getJSONObject("flow_state")
								.getJSONObject("historyProcess").clone();
					} else if (computer != null && JO.getJSONObject("computer").containsKey("flow_state")) {
						JSONObject _computer = JO.getJSONObject("computer").getJSONObject("flow_state")
								.getJSONObject("historyProcess");
						for (String key : computer.keySet()) {
							if (_computer.containsKey(key)) {
								computer.put(key, computer.getLongValue(key) + _computer.getLongValue(key));
							} else {
								computer.put(key, _computer.getLongValue(key));
							}
						}
					}
				}
			}
			res.put("reader", reader);
			res.put("writer", writer);
			res.put("computer", computer);
		} catch (EFException e) {
			setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
		}
		setResponse(RESPONSE_STATUS.Success, null, res);
	}

	/**
	 * get node environmental state
	 * 
	 * @param rq
	 * @param RR
	 */
	public void getStatus(Request rq, EFRequest RR) {
		int service_level = Integer.parseInt(GlobalParam.SystemConfig.get("service_level").toString());
		JSONObject dt = new JSONObject();
		dt.put("NODE_TYPE", GlobalParam.SystemConfig.getProperty("node_type"));
		dt.put("NODE_IP", GlobalParam.IP);
		dt.put("NODE_ID", GlobalParam.NODEID);
		dt.put("WRITE_BATCH", GlobalParam.WRITE_BATCH);
		dt.put("SERVICE_LEVEL", service_level);
		dt.put("LANG", GlobalParam.LANG);
		if (GlobalParam.DISTRIBUTE_RUN) {
			dt.put("STATUS", GlobalParam.INSTANCE_COORDER.distributeCoorder().getClusterState());
		} else {
			dt.put("STATUS", "running");
		}
		dt.put("VERSION", GlobalParam.VERSION);
		dt.put("TASKS", Resource.tasks.size());
		dt.put("THREAD_POOL_SIZE", Resource.threadPools.getPoolSize());
		dt.put("SYS_THREAD_POOL_SIZE", GlobalParam.STS_THREADPOOL_SIZE);
		dt.put("THREAD_ACTIVE_COUNT", Resource.threadPools.getActiveCount());
		dt.put("DISTRIBUTE_RUN", GlobalParam.DISTRIBUTE_RUN);
		dt.put("CPU_USAGE", SystemInfoUtil.getCpuUsage());
		dt.put("MEMORY_USAGE", SystemInfoUtil.getMemUsage());
		if (GlobalParam.DISTRIBUTE_RUN) {
			dt.put("SLAVES", GlobalParam.INSTANCE_COORDER.distributeCoorder().getNodeStatus());
		}
		setResponse(RESPONSE_STATUS.Success, null, dt);
	}

	/**
	 * Data source level delimited sequence
	 * 
	 * @param rq
	 * @param RR
	 */
	public void getInstanceSeqs(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance")) {
			try {
				String instance = RR.getStringParam("instance");
				InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instance);
				WarehouseParam dataMap = Resource.nodeConfig.getWarehouse()
						.get(instanceConfig.getPipeParams().getReadFrom());
				setResponse(RESPONSE_STATUS.Success, null, StringUtils.join(dataMap.getL1seq(), ","));
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, RR.getStringParam("instance") + " not exists!", null);
			}
		}
	}

	/**
	 * reset Instance full and increment running state
	 * 
	 * @param rq
	 */
	public void resetInstanceState(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance")) {
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
						EFFileUtil.getInstancePath(instance)[2]);
				setResponse(RESPONSE_STATUS.Success, RR.getStringParam("instance") + " reset Success!", null);
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.DataErr, RR.getStringParam("instance") + " not exists!", null);
			}
		}
	}

	/**
	 * close instance breaker state
	 * 
	 * @param rq
	 * @param RR
	 * @throws EFException
	 */
	public void resetBreaker(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance")) {
			try {
				Boolean state = EFMonitorUtil.resetBreaker(RR.getStringParam("instance"));
				if (state) {
					setResponse(RESPONSE_STATUS.Success, null, "");
				} else {
					setResponse(RESPONSE_STATUS.DataErr, "instance not exits!", null);
				}
			} catch (EFException e) {
				setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
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
		if (EFMonitorUtil.checkParams(this, RR, "instance")) {
			try {
				JSONObject JO = EFMonitorUtil.getInstanceInfo(RR.getStringParam("instance"), 7);
				if (JO.isEmpty()) {
					setResponse(RESPONSE_STATUS.DataErr, "instance not exits!", null);
				} else {
					setResponse(RESPONSE_STATUS.Success, null, JO);
				}
			} catch (EFException e) {
				setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
			}
		}
	}
	/**
	 * search instance datas.
	 * 
	 * @param rq
	 * @throws EFException
	 */
	public void searchInstanceData(Request rq, EFRequest RR) { 
		if (EFMonitorUtil.checkParams(this, RR, "instance")) {
			EFResponse rps = EFResponse.getInstance(); 
			String pipe = RR.getStringParam("instance"); 
			Map<String, InstanceConfig> configMap = Resource.nodeConfig.getSearchConfigs();
			EFRequest efRq = Common.getEFRequest(rq, rps);
			if (configMap.containsKey(pipe))  
				Resource.socketCenter.getSearcher(pipe,"","",false).startSearch(efRq,rps); 
			setResponse(RESPONSE_STATUS.Success, null, rps);
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
			instance.put("WriteTo", wt2);
			instance.put("OpenTrans", config.openTrans());
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
					edge.put("isconnect", true);
					if (node.getValue().getBoolean("OpenTrans") == false)
						edge.put("isconnect", false);
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
		setResponse(RESPONSE_STATUS.Success, null, result);
	}

	public void getInstanceXml(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance")) {
			String xmlPath = GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance") + "/task.xml";
			byte[] datas = EFDataStorer.getData(xmlPath, false);
			setResponse(RESPONSE_STATUS.Success, "", new String(datas));
		}
	}

	/**
	 * Direct coverage, therefore the content must be complete
	 * 
	 * @param rq
	 * @param RR
	 */
	public void updateInstanceXml(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance,content")) {
			String xmlPath = GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance") + "/task.xml";
			EFDataStorer.setData(xmlPath, new String(decoder.decode(RR.getStringParam("content"))));
			setResponse(RESPONSE_STATUS.Success, "update "+RR.getStringParam("instance")+" success", "");
		}
	}

	/**
	 * Modify task configure
	 * 
	 * @param rq
	 * @param RR
	 */
	public void setInstancePipeConfig(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance,param.name,param.value")) {
			if (Resource.nodeConfig.getInstanceConfigs().containsKey(RR.getStringParam("instance"))) {
				InstanceConfig tmp = Resource.nodeConfig.getInstanceConfigs().get(RR.getStringParam("instance"));
				try {
					String[] params = RR.getStringParam("param.name").split("\\.");
					if (params.length != 2)
						throw new EFException("param.name Must be within two levels.");
					Class<?> cls = null;
					Object obj = null;
					switch (params[0]) {
					case "TransParam":
						cls = tmp.getPipeParams().getClass();
						obj = tmp.getPipeParams();
						break;
					case "ReaderParam":
						cls = tmp.getReaderParams().getClass();
						obj = tmp.getReaderParams();
						break;
					case "ComputerParam":
						cls = tmp.getComputeParams().getClass();
						obj = tmp.getComputeParams();
						break;
					case "WriterParam":
						cls = tmp.getWriterParams().getClass();
						obj = tmp.getWriterParams();
						break;
					}
					Common.setConfigObj(obj, cls, params[1], RR.getStringParam("param.value"));
					if (GlobalParam.DISTRIBUTE_RUN)
						GlobalParam.INSTANCE_COORDER.distributeCoorder().updateNodeConfigs(
								RR.getStringParam("instance"), params[0], params[1], RR.getStringParam("param.value"));
					String xmlPath = GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance") + "/task.xml";
					try {
						PipeXMLUtil.ModifyNode(xmlPath, params[0] + ".param", params[1],
								RR.getStringParam("param.value"));
					} catch (EFException e) {
						setResponse(RESPONSE_STATUS.DataErr, RR.getStringParam("instance") + e.getMessage(), null);
					}
				} catch (Exception e) {
					setResponse(RESPONSE_STATUS.DataErr, e.getMessage(), null);
				}
			} else {
				setResponse(RESPONSE_STATUS.DataErr, RR.getStringParam("instance") + " not exists!", null);
			}
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
		for (Map.Entry<String, InstanceConfig> entry : nodes.entrySet()) {
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
			instance.put("WriteTo", config.getPipeParams().getWriteTo().replace(",", ";"));
			instance.put("OpenTrans", config.openTrans());
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
		setResponse(RESPONSE_STATUS.Success, null, rs);
	}

	/**
	 * run ElasticFlow CPU instruction program.
	 * 
	 * @param rq
	 * @throws EFException
	 */
	public void runCode(Request rq, EFRequest RR) {
		if (RR.getStringParam("script") != null && RR.getStringParam("script").contains("Track.cpuFree")) {
			try {
				ArrayList<InstructionTree> Instructions = Common.compileCodes(RR.getStringParam("script"),
						CPU.getUUID());
				for (InstructionTree Instruction : Instructions) {
					Instruction.depthRun(Instruction.getRoot());
				}
				setResponse(RESPONSE_STATUS.Success, "code run success!", null);
			} catch (EFException e) {
				setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
			}
		} else {
			setResponse(RESPONSE_STATUS.DataErr, "script not set or script grammer is not correct!", null);
		}
	}

	/**
	 * Perform the instance task immediately
	 * 
	 * @param rq
	 */
	public void runNow(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance,jobtype")) {
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
					setResponse(RESPONSE_STATUS.Success,
							"Writer " + RR.getStringParam("instance") + " job has been started now!", null);
				} else {
					setResponse(RESPONSE_STATUS.DataErr, "Writer " + RR.getStringParam("instance")
							+ " job not exists or run failed or had been stated!", null);
				}
			} else {
				setResponse(RESPONSE_STATUS.DataErr,
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
		if (EFMonitorUtil.checkParams(this, RR, "instance")) {
			removeInstance(RR.getStringParam("instance"));
			setResponse(RESPONSE_STATUS.Success, "Writer " + RR.getStringParam("instance") + " job have removed!",
					null);
		}
	}

	/**
	 * stop instance job.
	 * 
	 * @param rq
	 */
	public void stopInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance,type")) {
			GlobalParam.INSTANCE_COORDER.stopInstance(RR.getStringParam("instance"), RR.getStringParam("type"));
			setResponse(RESPONSE_STATUS.Success,
					"Writer " + RR.getStringParam("instance") + " job stopped successfully!", null);
		}
	}

	/**
	 * resume instance job.
	 * 
	 * @param rq
	 */
	public void resumeInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance,type")) {
			GlobalParam.INSTANCE_COORDER.resumeInstance(RR.getStringParam("instance"), RR.getStringParam("type"));
			setResponse(RESPONSE_STATUS.Success,
					"Writer " + RR.getStringParam("instance") + " job resumed successfully!", null);
		}
	}

	/**
	 * reload instance configure rebuild instance in memory
	 * 
	 * @param rq instance=xx&reset=true|false&runtype=1 reset true will clear all
	 *           instance settings. runType=-1 Use the original task run type
	 * 
	 * @throws EFException
	 * 
	 */
	public void reloadInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance,runtype,reset")) {
			String instance = RR.getStringParam("instance");
			String reset = RR.getStringParam("reset");
			String runType = RR.getStringParam("runtype");
			if (!Resource.nodeConfig.getInstanceConfigs().containsKey(instance)) {
				setResponse(RESPONSE_STATUS.DataErr, instance + " not exists!", null);
			} else {
				try {
					if (runType.equals("-1"))
						runType = String
								.valueOf(Resource.nodeConfig.getInstanceConfigs().get(instance).getInstanceType());
					EFMonitorUtil.reloadInstance(instance, reset, runType);
					setResponse(RESPONSE_STATUS.Success,
							RR.getStringParam("instance") + " reload instance settings success!", null);
				} catch (EFException e) {
					setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
				}
			}
		}
	}

	/**
	 * push instance to system
	 * 
	 * @param rq
	 * @param RR
	 */
	public void addInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance,content,level")) {
			String xmlPath = GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance") + "/task.xml";
			try {
				EFDataStorer.createPath(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance"), false);
				EFDataStorer.setData(xmlPath, new String(decoder.decode(RR.getStringParam("content"))));
				EFMonitorUtil.addInstanceToSystem(RR.getStringParam("instance"), RR.getStringParam("level"));
				EFMonitorUtil.saveNodeConfig();
				setResponse(RESPONSE_STATUS.Success,
						RR.getStringParam("instance") + " save and push to node " + GlobalParam.IP + " success!", null);
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
			}
		}
	}

	public void cloneInstance(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance,new_instance_name")) {
			EFFileUtil.copyFolder(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance"),
					GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("new_instance_name"));
			EFFileUtil.delFile(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("new_instance_name") + "/"
					+ GlobalParam.JOB_FULLINFO_PATH);
			EFFileUtil.delFile(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("new_instance_name") + "/"
					+ GlobalParam.JOB_INCREMENTINFO_PATH);
			setResponse(RESPONSE_STATUS.Success, RR.getStringParam("new_instance_name") + " clone success!", null);
		}
	}

	/**
	 * add instance setting into system and add it to configure file also.
	 * 
	 * @param rq
	 * @param RR
	 */
	public void addInstanceToSystem(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance,level")) {
			EFMonitorUtil.addInstanceToSystem(RR.getStringParam("instance"), RR.getStringParam("level"));
			try {
				EFMonitorUtil.saveNodeConfig();
				setResponse(RESPONSE_STATUS.Success,
						RR.getStringParam("instance") + " add to node " + GlobalParam.IP + " success!", null);
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
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
		if (EFMonitorUtil.checkParams(this, RR, "instance")) {
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
							wfs.PREPARE(false, false, false);
							if (wfs.ISLINK()) {
								wfs.removeInstance(instance,
										GlobalParam.TASK_COORDER.getStoreIdFromSave(instance, L1seq, true, false));
								wfs.REALEASE(false, false);
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
				setResponse(RESPONSE_STATUS.Success, "delete " + _instance + " success!", null);
			} else {
				setResponse(RESPONSE_STATUS.CodeException, "delete " + _instance + " failed!", null);
			}
		}
	}

	/**
	 * Update resource node information
	 * 
	 * @param resourceData
	 * @param isDel
	 * @return
	 */
	private boolean updateResourceXml(JSONObject resourceData, boolean isDel) {
		String pondPath = GlobalParam.DATAS_CONFIG_PATH + "/" + GlobalParam.SystemConfig.getProperty("pond");
		try {
			byte[] resourceXml = EFDataStorer.getData(pondPath, false);
			String rname = resourceData.getString("name");
			SAXReader reader = new SAXReader();
			Document doc = reader.read(new ByteArrayInputStream(resourceXml));
			Element root = doc.getRootElement();
			List<?> socketlist = root.elements();
			boolean isExist = false;
			for (Iterator<?> it = socketlist.iterator(); it.hasNext();) {
				Element socket = (Element) it.next();
				if (rname.equals(socket.element("name").getTextTrim())) {
					if (isDel) {
						socketlist.remove(socket);
						break;
					}
					isExist = true;
					List<?> itemlist = socket.elements();
					for (Iterator<?> sitem = itemlist.iterator(); sitem.hasNext();) {
						Element socketContent = (Element) sitem.next();
						if (resourceData.getString(socketContent.getName()) != null
								&& resourceData.getString(socketContent.getName()) != "") {
							if (socketContent.getText() != resourceData.getString(socketContent.getName())
									&& !socketContent.getText()
											.equals(resourceData.getString(socketContent.getName()))) {
								socketContent.setText(resourceData.getString(socketContent.getName()));
							}
							resourceData.remove(socketContent.getName());

						} else if (resourceData.getString(socketContent.getName()) == null
								|| resourceData.getString(socketContent.getName()) == "") {
							socket.remove(socketContent);
							resourceData.remove(socketContent.getName());
						}
					}
					if (resourceData.size() > 0) {
						for (Map.Entry<String, Object> entry : resourceData.entrySet()) {
							Element socketinfo = socket.addElement(entry.getKey());
							socketinfo.setText(entry.getValue().toString());
						}
					}
				} else {
					continue;
				}
			}
			if (!isExist && !isDel) {
				Element newelement = root.addElement("socket");
				for (Map.Entry<String, Object> entry : resourceData.entrySet()) {
					Element element = newelement.addElement(entry.getKey());
					element.setText(entry.getValue().toString());
				}
			}
			EFDataStorer.setData(pondPath, Common.formatXml(doc));
		} catch (Exception e) {
			Common.LOG.error("update resource node information to {} exception", pondPath, e);
			setResponse(RESPONSE_STATUS.CodeException, "save resource exception " + e.getMessage(), null);
			return false;
		}
		return true;
	}

	/**
	 * remove instance stop all jobs and remove from configure file.
	 * 
	 * @param instance
	 */
	private void removeInstance(String instance) {
		if (GlobalParam.DISTRIBUTE_RUN) {
			GlobalParam.INSTANCE_COORDER.distributeCoorder().removeInstanceFromCluster(instance, false);
			GlobalParam.INSTANCE_COORDER.removeInstance(instance, false);
		} else {
			GlobalParam.INSTANCE_COORDER.removeInstance(instance, true);
		}
		try {
			EFMonitorUtil.saveNodeConfig();
		} catch (Exception e) {
			setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
		}
	}
}
