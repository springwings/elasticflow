/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.node.monitor;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.config.GlobalParam.RESOURCE_TYPE;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.model.EFRequest;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.InstructionTree;
import org.elasticflow.node.CPU;
import org.elasticflow.node.EFNode;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFLoader;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.PipeXMLUtil;
import org.elasticflow.util.SystemInfoUtil;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.yarn.Resource;
import org.mortbay.jetty.Request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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

	public static Decoder decoder = Base64.getDecoder();

	public HashMap<String, String> instanceActions = new HashMap<String, String>();

	private InstanceManage instanceManage;

	HashMap<String, String> nodeActions = new HashMap<String, String>() {
		private static final long serialVersionUID = -8313429841889556616L;
		{
			// node manage
			put("getnodeconfig", "getNodeConfig");
			put("setnodeconfig", "setNodeConfig");
			put("getnodeconfigcontent", "getNodeConfigContent");
			put("updatenodeconfigcontent", "updateNodeConfigContent");
			put("globalstatus", "globalStatus");
			put("getstatus", "getStatus");
			put("gethosts", "getHosts");
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
			// other manage
			put("getresource", "getResource");
			put("getresources", "getResources");
			put("getmodules", "getModules");
			put("unloadmodule", "unloadModule");
			put("startmodule", "startModule");
			put("getresourcexml", "getResourcexml");
			put("updateresource", "updateResource");
			put("addresource", "addResource");
			put("removeresource", "removeResource");
			put("setinstancepipeconfig", "setInstancePipeConfig");
			put("geterrorlog", "getErrorLog");
			put("getsystemlog", "getSystemLog");
		}
	};

	public NodeMonitor() {
		instanceManage = new InstanceManage(this);
	}

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
			if (RR.getParams().get("ac") != null) {
				String method = RR.getStringParam("ac").toLowerCase();
				if (this.instanceActions.containsKey(method)) {
					Class<?> clazz = instanceManage.getClass();
					Method m = clazz.getMethod(this.instanceActions.get(method), Request.class, EFRequest.class);
					m.invoke(instanceManage, rq, RR);
				} else if (this.nodeActions.containsKey(method)) {
					Method m = NodeMonitor.class.getMethod(this.nodeActions.get(RR.getStringParam("ac").toLowerCase()),
							Request.class, EFRequest.class);
					m.invoke(this, rq, RR);
				} else {
					RS.setStatus("action not exists!", RESPONSE_STATUS.ParameterErr);
				}
				RS.setStatus(this.response_info, this.response_status);
				RS.setPayload(this.response_data);
			} else {
				RS.setPayload(this.nodeActions);
				RS.setStatus("Example: /efm.doaction?ac=getStatus", RESPONSE_STATUS.ParameterErr);
			}
		} catch (Exception e) {
			RS.setStatus("Actions Exception!", RESPONSE_STATUS.CodeException);
			Common.LOG.error("Management Operations {} exception", RR.getParams().get("ac"), e);
		}
	}

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
					instanceManage.removeInstance(entry.getKey());
				}
				if (instanceConfig.getPipeParams().getWriteTo().equals(name)) {
					instanceManage.removeInstance(entry.getKey());
				}
				if (instanceConfig.getPipeParams().getSearchFrom().equals(name)) {
					instanceManage.removeInstance(entry.getKey());
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
	 * scan all modules
	 * 
	 * @param rq
	 * @param RR
	 */
	public void getModules(Request rq, EFRequest RR) {
		JSONArray res = PipeXMLUtil.getModules();
		setResponse(RESPONSE_STATUS.Success, "", res);
	}

	public void unloadModule(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "module")) {
			boolean res = PipeXMLUtil.unloadModule(RR.getStringParam("module"));
			setResponse(RESPONSE_STATUS.Success, "", res);
		}

	}

	/**
	 * start Module to instance
	 * 
	 * @param rq
	 * @param RR
	 */
	public void startModule(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instancename,module,readfrom,writeto")) {
			if (!RR.getStringParam("instancename").equals(RR.getStringParam("module"))) {
				EFFileUtil.copyFolder(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("module"),
						GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instancename"));
			}
			String xmlPath = GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instancename") + "/task.xml";
			try {
				int level = 1;
				PipeXMLUtil.ModifyNode(xmlPath, "TransParam.param", "readfrom", RR.getStringParam("readfrom"));
				PipeXMLUtil.ModifyNode(xmlPath, "TransParam.param", "writeto", RR.getStringParam("writeto"));
				if (RR.getStringParam("compute") != "") {
					PipeXMLUtil.ModifyNode(xmlPath, "ComputerParam.param", "api", RR.getStringParam("compute"));
					level += 2;
				}
				EFMonitorUtil.addInstanceToSystem(RR.getStringParam("instancename"), String.valueOf(level));
				try {
					EFMonitorUtil.saveNodeConfig();
					setResponse(RESPONSE_STATUS.Success,
							RR.getStringParam("instancename") + " add to node " + GlobalParam.IP + " success!", null);
				} catch (Exception e) {
					setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
				}
			} catch (EFException e) {
				setResponse(RESPONSE_STATUS.DataErr, RR.getStringParam("instance") + e.getMessage(), null);
			}
			setResponse(RESPONSE_STATUS.Success, "", null);
		}
	}

	/**
	 * read Resources
	 * 
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
			res.getJSONObject(entry.getKey()).put("remark", wp.getRemarks());
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
				if (RR.getStringParam("name").equals(entry.getKey())) {
					WarehouseParam wp = entry.getValue();
					res.put(entry.getKey(), new JSONObject());
					res.getJSONObject(entry.getKey()).put("name", entry.getKey());
					res.getJSONObject(entry.getKey()).put("hosts", wp.getHost());
					res.getJSONObject(entry.getKey()).put("type", wp.getType());
					res.getJSONObject(entry.getKey()).put("defaultValue", wp.getDefaultValue());
					res.getJSONObject(entry.getKey()).put("user", wp.getUser());
					res.getJSONObject(entry.getKey()).put("port", wp.getPort());
					res.getJSONObject(entry.getKey()).put("remarks", wp.getRemarks());
					res.getJSONObject(entry.getKey()).put("L1seq", wp.getL1seq());
					res.getJSONObject(entry.getKey()).put("customParams", wp.getCustomParams());
					JSONObject pools = new JSONObject();
					for (String seq : wp.getL1seq()) {
						pools.put((seq == "" ? "DEFAULT" : seq),
								EFMonitorUtil.getConnectionStatus(wp.getPoolName(seq)));
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
					Common.setConfigObj(o, WarehouseParam.class, key, jsonObject.getString(key), null);
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
			int error_ignore = 0;
			int error_dispose = 0;
			int error_breakoff = 0;
			int error_termination = 0;
			for (Map.Entry<String, Object> entry : nodeinfos.entrySet()) {
				@SuppressWarnings("unchecked")
				HashMap<String, Object> datas = (HashMap<String, Object>) entry.getValue();
				mem_use += Double.parseDouble(datas.get("MEMORY_USAGE").toString());
				cpu_use += Double.parseDouble(datas.get("CPU_USAGE").toString());
				total_mem += Double.parseDouble(datas.get("MEMORY").toString());
				error_ignore += Double.parseDouble(datas.get("ERROR_IGNORE").toString());
				error_dispose += Double.parseDouble(datas.get("ERROR_DISPOSE").toString());
				error_breakoff += Double.parseDouble(datas.get("ERROR_BREAKOFF").toString());
				error_termination += Double.parseDouble(datas.get("ERROR_TERMINATION").toString());
			}
			res.put("memory_usage",
					mem_use / (GlobalParam.INSTANCE_COORDER.distributeCoorder().getNodes().size() + 1.));
			res.put("cpu_usage", cpu_use / (GlobalParam.INSTANCE_COORDER.distributeCoorder().getNodes().size() + 1.));
			res.put("total_memory", total_mem);
			res.put("error_ignore", error_ignore);
			res.put("error_dispose", error_dispose);
			res.put("error_breakoff", error_breakoff);
			res.put("error_termination", error_termination);
			res.put("system_start_time", Common.FormatTime(GlobalParam.SYS_START_TIME));
		} else {
			res.put("node_num", 1);
			res.put("total_memory", SystemInfoUtil.getMemTotal());
			res.put("memory_usage", SystemInfoUtil.getMemUsage());
			res.put("cpu_usage", SystemInfoUtil.getCpuUsage());
			res.put("error_ignore", Resource.getErrorStates(ELEVEL.Ignore));
			res.put("error_dispose", Resource.getErrorStates(ELEVEL.Dispose));
			res.put("error_breakoff", Resource.getErrorStates(ELEVEL.BreakOff));
			res.put("error_termination", Resource.getErrorStates(ELEVEL.Termination));
			res.put("system_start_time", Common.FormatTime(GlobalParam.SYS_START_TIME));
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
		res.put("log_store_path", GlobalParam.lOG_STORE_PATH);
		res.put("datas_config_path", GlobalParam.DATAS_CONFIG_PATH);
		res.put("plugin_path", GlobalParam.pluginPath);
		res.put("health", health ? "正常" : "异常");
		// reader computer writer data statistics
		try {
			JSONObject reader = new JSONObject();
			JSONObject computer = new JSONObject();
			JSONObject writer = new JSONObject();
			for (Map.Entry<String, InstanceConfig> entry : Resource.nodeConfig.getInstanceConfigs().entrySet()) {
				if (!entry.getValue().openTrans())
					continue;
				JSONObject JO = EFMonitorUtil.getInstanceInfo(entry.getKey(), 2);
				if (!JO.isEmpty()) {
					if (reader == null && JO.getJSONObject("reader").containsKey("flow_state")) {
						reader = (JSONObject) JO.getJSONObject("reader").getJSONObject("flow_state")
								.getJSONObject("historyProcess").clone();
					} else if (reader != null && JO.getJSONObject("reader").containsKey("flow_state")) {
						JSONObject _reader = JO.getJSONObject("reader").getJSONObject("flow_state")
								.getJSONObject("historyProcess");
						for (String key : _reader.keySet()) {
							if (reader.containsKey(key)) {
								reader.put(key, reader.getLongValue(key) + _reader.getLongValue(key));
							} else {
								reader.put(key, _reader.getLongValue(key));
							}
						}
					}

					if (writer == null && JO.getJSONObject("writer").containsKey("flow_state"))
						writer = (JSONObject) JO.getJSONObject("writer").getJSONObject("flow_state")
								.getJSONObject("historyProcess").clone();
					else if (writer != null && JO.getJSONObject("writer").containsKey("flow_state")) {
						JSONObject _writer = JO.getJSONObject("writer").getJSONObject("flow_state")
								.getJSONObject("historyProcess");
						for (String key : _writer.keySet()) {
							if (writer.containsKey(key)) {
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
						for (String key : _computer.keySet()) {
							if (computer.containsKey(key)) {
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

	public void getHosts(Request rq, EFRequest RR) {
		JSONArray dt = new JSONArray();
		dt.add(GlobalParam.IP);
		if (GlobalParam.DISTRIBUTE_RUN) {
			for (EFNode node : GlobalParam.INSTANCE_COORDER.distributeCoorder().getNodes()) {
				dt.add(node.getIp());
			}
		}
		setResponse(RESPONSE_STATUS.Success, null, dt);
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
		dt.put("SYS_THREAD_POOL_SIZE", GlobalParam.SYS_THREADPOOL_SIZE);
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
					Common.setConfigObj(obj, cls, params[1], RR.getStringParam("param.value"), null);
					if (GlobalParam.DISTRIBUTE_RUN)
						GlobalParam.INSTANCE_COORDER.distributeCoorder().updateInstanceConfig(
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

	public void getErrorLog(Request rq, EFRequest RR) {
		setResponse(RESPONSE_STATUS.Success, "", EFFileUtil.readText(GlobalParam.ERROR_lOG_STORE_PATH, "utf-8", false));
	}

	public void getSystemLog(Request rq, EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "lines")) {
			String ip = RR.getStringParam("ip");
			if (ip != "" && !ip.equals(GlobalParam.IP)) {
				setResponse(RESPONSE_STATUS.Success, "",
						GlobalParam.INSTANCE_COORDER.distributeCoorder().getNodeLogs(ip, RR.getIntParam("lines")));
			} else {
				setResponse(RESPONSE_STATUS.Success, "",
						EFFileUtil.readLastNLines(GlobalParam.lOG_STORE_PATH, RR.getIntParam("lines")));
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

}
