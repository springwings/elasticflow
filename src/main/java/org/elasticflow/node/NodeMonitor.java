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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.MECHANISM;
import org.elasticflow.config.GlobalParam.RESOURCE_TYPE;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.InstructionTree;
import org.elasticflow.param.pipe.InstructionParam;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.reader.service.HttpReaderService;
import org.elasticflow.searcher.service.SearcherService;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFLoader;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.util.PipeXMLUtil;
import org.elasticflow.util.SystemInfoUtil;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.writer.WriterFlowSocket;
import org.elasticflow.yarn.Resource;
import org.mortbay.jetty.Request;
import org.springframework.beans.factory.annotation.Autowired;

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

	@Autowired
	private SearcherService SearcherService;

	@Autowired
	private HttpReaderService HttpReaderService;

	private RESPONSE_STATUS response_status = RESPONSE_STATUS.CodeException;

	private String response_info;

	private Object response_data;

	private HashSet<String> actions = new HashSet<String>() {
		private static final long serialVersionUID = -8313429841889556616L;
		{
			// node manage
			add("addResource");
			add("removeResource");
			add("getNodeConfig");
			add("setNodeConfig");
			add("getStatus");
			add("getInstances");
			add("startSearcherService");
			add("stopSearcherService");
			add("startHttpReaderServiceService");
			add("stopHttpReaderServiceService");
			add("restartNode");
			add("loadHandler");
			// instance manage
			add("cloneInstance");
			add("resetInstanceState");
			add("getInstanceSeqs");
			add("reloadInstanceConfig");
			add("runNow");
			add("addInstance");
			add("stopInstance");
			add("resumeInstance");
			add("removeInstance");
			add("deleteInstanceData");
			add("getInstanceInfo");
			add("runCode");
			//pipe xml-config manage
			add("setInstancePipeConfig");
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

	public void ac(Request rq, EFResponse RS) {
		try {  
			if (this.actions.contains(rq.getParameter("ac"))) {
				Method m = NodeMonitor.class.getMethod(rq.getParameter("ac"), Request.class);
				m.invoke(this, rq);
				RS.setStatus(this.response_info, this.response_status);
				RS.setPayload(this.response_data);
			} else {
				RS.setStatus("Actions Not Exists!", RESPONSE_STATUS.ParameterErr);
			}
		} catch (Exception e) {
			RS.setStatus("Actions Exception!", RESPONSE_STATUS.CodeException);
			Common.LOG.error("ac " + rq.getParameter("ac") + " Exception ", e);
		}
	}
	
	

	/**
	 * Be care full,this will remove all relative instance
	 * 
	 * @param rq
	 */
	public void removeResource(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "name,type")) {
			String name = rq.getParameter("name");
			RESOURCE_TYPE type = RESOURCE_TYPE.valueOf(rq.getParameter("type").toUpperCase());
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
			updateResourceXml(type.name(), jsonObject, true);
		}
	}

	/**
	 * @param socket resource configs json string
	 */
	public void addResource(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "socket,type")) {
			JSONObject jsonObject = JSON.parseObject(rq.getParameter("socket"));
			RESOURCE_TYPE type = RESOURCE_TYPE.valueOf(rq.getParameter("type").toUpperCase());
			Object o = null;
			Set<String> iter = jsonObject.keySet();
			try {
				switch (type) {				
				case WAREHOUSE:
					o = new WarehouseParam();
					for (String key : iter) {
						Common.setConfigObj(o, WarehouseParam.class, key, jsonObject.getString(key));
					}
					break;
				case INSTRUCTION:
					o = new InstructionParam();
					for (String key : iter) {
						Common.setConfigObj(o, InstructionParam.class, key, jsonObject.getString(key));
					}
					break;
				}
				if (o != null) {
					Resource.nodeConfig.addSource(type, o);
					setResponse(RESPONSE_STATUS.Success, "add Resource to node success!", null);
					updateResourceXml(type.name(), jsonObject, false);
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
	public void getNodeConfig(Request rq) {
		setResponse(RESPONSE_STATUS.Success, "", GlobalParam.StartConfig);
	}

	/**
	 * set node start configure parameters,will auto write into file.
	 * 
	 * @param k    property key
	 * @param v    property value
	 * @param type action type,set/remove
	 */
	public void setNodeConfig(Request rq) {
		if (rq.getParameter("k") != null && rq.getParameter("v") != null && rq.getParameter("type") != null) {
			if (rq.getParameter("type").equals("set")) {
				GlobalParam.StartConfig.setProperty(rq.getParameter("k"), rq.getParameter("v"));
			} else {
				GlobalParam.StartConfig.remove(rq.getParameter("k"));
			}
			try {
				EFMonitorUtil.saveNodeConfig();
				setResponse(RESPONSE_STATUS.Success, "Config set success!", null);
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, "Config save Exception " + e.getMessage(), null);
			}
		} else {
			setResponse(RESPONSE_STATUS.DataErr, "Config parameters k v or type not exists!", null);
		}
	}

	/**
	 * restart node
	 * 
	 * @param rq
	 */
	public void restartNode(Request rq) {
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				EFNodeUtil.runShell(GlobalParam.StartConfig.getProperty("restart_shell"));
			}
		});
		thread.start();
		setResponse(RESPONSE_STATUS.CodeException, "current node is in restarting...", null);
	}

	/**
	 * Loading Java handler classes in real time only support no dependency handler
	 * like org.elasticflow.writerUnit.handler org.elasticflow.reader.handler
	 * org.elasticflow.searcher.handler
	 * 
	 * @param rq
	 */
	public void loadHandler(Request rq) {
		if (rq.getParameter("path") != null && rq.getParameter("name") != null) {
			try {
				new EFLoader(rq.getParameter("path")).loadClass(rq.getParameter("name"));
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
	public void stopHttpReaderServiceService(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if ((service_level & 4) > 0) {
			service_level -= 4;
		}
		if (HttpReaderService.close()) {
			setResponse(RESPONSE_STATUS.Success, "Stop Searcher Service Successed!", null);
		} else {
			setResponse(RESPONSE_STATUS.CodeException, "Stop Searcher Service Failed!", null);
		}
	}

	/**
	 * start node http reader pipe service
	 * 
	 * @param rq
	 */
	public void startHttpReaderServiceService(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if ((service_level & 4) == 0) {
			service_level += 4;
			HttpReaderService.start();
		}
		setResponse(RESPONSE_STATUS.Success, "Start Searcher Service Successed!", null);
	}

	/**
	 * stop node searcher service
	 * 
	 * @param rq
	 */
	public void stopSearcherService(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if ((service_level & 1) > 0) {
			service_level -= 1;
		}
		if (SearcherService.close()) {
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
	public void startSearcherService(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if ((service_level & 1) == 0) {
			service_level += 1;
			SearcherService.start();
		}
		setResponse(RESPONSE_STATUS.Success, "Start Searcher Service Successed!", null);
	}

	/**
	 * get node environmental state.
	 * 
	 * @param rq
	 */
	public void getStatus(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		JSONObject dt = new JSONObject();
		dt.put("NODE_TYPE", GlobalParam.StartConfig.getProperty("node_type"));
		dt.put("WRITE_BATCH", GlobalParam.WRITE_BATCH);
		dt.put("SERVICE_LEVEL", service_level);
		dt.put("STATUS", "running");
		dt.put("VERSION", GlobalParam.VERSION);
		dt.put("TASKS", Resource.tasks.size());
		try {
			dt.put("CPU", SystemInfoUtil.getCpuUsage());
			dt.put("MEMORY", SystemInfoUtil.getMemUsage());
		} catch (Exception e) {
			Common.LOG.error(" getStatus Exception ", e);
		}
		setResponse(RESPONSE_STATUS.Success, null, dt);
	}

	/**
	 * Data source level delimited sequence
	 * 
	 * @param rq
	 */
	public void getInstanceSeqs(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance")) {
			try {
				String instance = rq.getParameter("instance");
				InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instance);
				WarehouseParam dataMap = Resource.nodeConfig.getWarehouse()
						.get(instanceConfig.getPipeParams().getReadFrom());				
				setResponse(RESPONSE_STATUS.Success, null, StringUtils.join(dataMap.getL1seq(), ","));
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, rq.getParameter("instance") + " not exists!", null);
			}
		}
	}

	/**
	 * reset Instance full and increment running state
	 * 
	 * @param rq
	 */
	public void resetInstanceState(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance")) {
			try {
				String instance = rq.getParameter("instance");
				String val = "0";
				if (rq.getParameterMap().get("set_value") != null)
					val = rq.getParameter("set_value");
				String[] L1seqs = EFMonitorUtil.getInstanceL1seqs(instance);
				for (String L1seq : L1seqs) {
					GlobalParam.TASK_COORDER.batchUpdateSeqPos(instance,val);
					GlobalParam.TASK_COORDER.saveTaskInfo(instance, L1seq, GlobalParam.TASK_COORDER.getStoreIdFromSave(instance, L1seq, false),
							GlobalParam.JOB_INCREMENTINFO_PATH);
				}
				setResponse(RESPONSE_STATUS.Success, rq.getParameter("instance") + " reset Success!", null);
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.DataErr, rq.getParameter("instance") + " not exists!", null);
			}
		}
	}

	/**
	 * get instance detail informations.
	 * 
	 * @param rq
	 */
	public void getInstanceInfo(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance")) {
			JSONObject JO = EFMonitorUtil.getInstanceInfo(rq.getParameter("instance"),7);
			if (JO.isEmpty()) {
				setResponse(RESPONSE_STATUS.DataErr, "instance not exits!", null);				
			} else {
				setResponse(RESPONSE_STATUS.Success, null, JO);				
			}
		}
	}
	
	public void setInstancePipeConfig(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance,param.name,param.value")) {
			if (Resource.nodeConfig.getInstanceConfigs().containsKey(rq.getParameter("instance"))){
				InstanceConfig tmp = Resource.nodeConfig.getInstanceConfigs().get(rq.getParameter("instance"));				
				try {
					String[] params = rq.getParameter("param.name").split("\\.");
					if(params.length!=2)
						throw new EFException("param.name Must be within two levels.");
					Class<?> cls = null;
					switch(params[0]) {
					case "TransParam":
						cls = tmp.getPipeParams().getClass();
						break;
					case "ReadParam":
						cls = tmp.getReadParams().getClass();
						break;
					case "ComputeParam":
						cls = tmp.getComputeParams().getClass();
						break;
					case "WriteParam":
						cls = tmp.getWriterParams().getClass();
						break;	
					}
					
					Common.setConfigObj(tmp.getPipeParams(), cls, params[1], rq.getParameter("param.value"));	
					String xmlPath = GlobalParam.INSTANCE_PATH + "/" + rq.getParameter("instance")+"/task.xml";
					
					try {
						PipeXMLUtil.ModifyNode(xmlPath, params[0]+".param", params[1], rq.getParameter("param.value"));
					} catch (EFException e) {
						setResponse(RESPONSE_STATUS.DataErr, rq.getParameter("instance")
								+ e.getMessage(), null);
					} 
				} catch (Exception e) {
					setResponse(RESPONSE_STATUS.DataErr, e.getMessage(), null);
				}
			}else {
				setResponse(RESPONSE_STATUS.DataErr,rq.getParameter("instance") + " not exists!", null);
			}
		}
	}

	/**
	 * get all instances info
	 * 
	 * @param rq
	 */
	public void getInstances(Request rq) {
		Map<String, InstanceConfig> nodes = Resource.nodeConfig.getInstanceConfigs();
		HashMap<String, List<JSONObject>> rs = new HashMap<String, List<JSONObject>>();
		for (Map.Entry<String, InstanceConfig> entry : nodes.entrySet()) {
			InstanceConfig config = entry.getValue();
			JSONObject instance = new JSONObject();
			instance.put("instance", entry.getKey());
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
			instance.put("openTrans", config.openTrans());
			instance.put("IsMaster", config.getPipeParams().isMaster());
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
	public void runCode(Request rq) throws EFException {
		if (rq.getParameter("script") != null && rq.getParameter("script").contains("Track.cpuFree")) {
			ArrayList<InstructionTree> Instructions = Common.compileCodes(rq.getParameter("script"), CPU.getUUID());
			for (InstructionTree Instruction : Instructions) {
				Instruction.depthRun(Instruction.getRoot());
			}
			setResponse(RESPONSE_STATUS.Success, "code run success!", null);
		} else {
			setResponse(RESPONSE_STATUS.DataErr, "script not set or script grammer is not correct!", null);
		}
	}

	/**
	 * Perform the instance task immediately
	 * 
	 * @param rq
	 */
	public void runNow(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance,jobtype")) {
			if (Resource.nodeConfig.getInstanceConfigs().containsKey(rq.getParameter("instance"))
					&& Resource.nodeConfig.getInstanceConfigs().get(rq.getParameter("instance")).openTrans()) {
				boolean state = Resource.FlOW_CENTER.runInstanceNow(rq.getParameter("instance"),
						rq.getParameter("jobtype"), true);
				if (state) {
					setResponse(RESPONSE_STATUS.Success,
							"Writer " + rq.getParameter("instance") + " job has been started now!", null);
				} else {
					setResponse(RESPONSE_STATUS.DataErr, "Writer " + rq.getParameter("instance")
							+ " job not exists or run failed or had been stated!", null);
				}
			} else {
				setResponse(RESPONSE_STATUS.DataErr,
						"Writer " + rq.getParameter("instance") + " job not open in this node!Run start faild!", null);
			}
		}
	}

	public void removeInstance(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance")) {
			removeInstance(rq.getParameter("instance"));
			setResponse(RESPONSE_STATUS.Success, "Writer " + rq.getParameter("instance") + " job have removed!", null);
		}
	}

	/**
	 * stop instance job.
	 * 
	 * @param rq
	 */
	public void stopInstance(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance,type")) {
			GlobalParam.INSTANCE_COORDER.stopInstance(rq.getParameter("instance"),rq.getParameter("type"));
			setResponse(RESPONSE_STATUS.Success, "Writer " + rq.getParameter("instance") + " job have stopped!", null);
		}
	}

	/**
	 * resume instance job.
	 * 
	 * @param rq
	 */
	public void resumeInstance(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance,type")) {
			GlobalParam.INSTANCE_COORDER.resumeInstance(rq.getParameter("instance"),rq.getParameter("type"));
			setResponse(RESPONSE_STATUS.Success, "Writer " + rq.getParameter("instance") + " job have resumed!", null);
		}
	}

	/**
	 * reload instance configure,auto rebuild instance in memory
	 * 
	 * @param rq instance=xx&reset=true|false reset true will recreate the instance
	 *           in java from instance configure.
	 */
	public void reloadInstanceConfig(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance")) {
			EFMonitorUtil.controlInstanceState(rq.getParameter("instance"), STATUS.Stop, true);
			int type = Resource.nodeConfig.getInstanceConfigs().get(rq.getParameter("instance")).getInstanceType();
			String instanceConfig = rq.getParameter("instance");
			if (type > 0) {
				instanceConfig = rq.getParameter("instance") + ":" + type;
			} else {
				if (!Resource.nodeConfig.getInstanceConfigs().containsKey(rq.getParameter("instance")))
					setResponse(RESPONSE_STATUS.DataErr, rq.getParameter("instance") + " not exists!", null);
			}
			Resource.FLOW_INFOS.remove(rq.getParameter("instance"), JOB_TYPE.FULL.name());
			Resource.FLOW_INFOS.remove(rq.getParameter("instance"), JOB_TYPE.INCREMENT.name());
			if (rq.getParameter("reset") != null && rq.getParameter("reset").equals("true")
					&& rq.getParameter("instance").length() > 2) {
				Resource.nodeConfig.loadConfig(instanceConfig, true);
			} else {
				String alias = Resource.nodeConfig.getInstanceConfigs().get(rq.getParameter("instance")).getAlias();
				Resource.nodeConfig.getSearchConfigs().remove(alias);
				Resource.nodeConfig.loadConfig(instanceConfig, false);
				Resource.FlOW_CENTER.removeInstance(rq.getParameter("instance"), true, true);
			}
			EFMonitorUtil.rebuildFlowGovern(instanceConfig,!GlobalParam.DISTRIBUTE_RUN);
			EFMonitorUtil.controlInstanceState(rq.getParameter("instance"), STATUS.Ready, true);
			setResponse(RESPONSE_STATUS.Success, rq.getParameter("instance") + " reload Config Success!", null);
		}
	}

	public void cloneInstance(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance,new_instance_name")) {
			EFFileUtil.copyFolder(GlobalParam.INSTANCE_PATH + "/" + rq.getParameter("instance"),
					GlobalParam.INSTANCE_PATH + "/" + rq.getParameter("new_instance_name"));
			EFFileUtil.delFile(GlobalParam.INSTANCE_PATH + "/" + rq.getParameter("new_instance_name") + "/batch");
			EFFileUtil.delFile(GlobalParam.INSTANCE_PATH + "/" + rq.getParameter("new_instance_name") + "/full_info");
			setResponse(RESPONSE_STATUS.Success, rq.getParameter("new_instance_name") + " clone Success!", null);
		}
	}

	/**
	 * add instance into system and add to configure also.
	 * 
	 * @param rq instance parameter example,instanceName:1
	 */
	public void addInstance(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance")) {
			GlobalParam.INSTANCE_COORDER.addInstance(rq.getParameter("instance"));
			EFMonitorUtil.addConfigInstances(rq.getParameter("instance"));
			try {
				EFMonitorUtil.saveNodeConfig();
				setResponse(RESPONSE_STATUS.Success, rq.getParameter("instance") + " add to node " + GlobalParam.IP + " Success!",
						null);
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
			}
		}
	}

	/**
	 * delete Instance Data through alias or Instance data name
	 * 
	 * @param alias
	 * @return
	 */
	public void deleteInstanceData(Request rq) {
		if (EFMonitorUtil.checkParams(this, rq, "instance")) {
			String _instance = rq.getParameter("instance");
			Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
			boolean state = true;
			for (Map.Entry<String, InstanceConfig> ents : configMap.entrySet()) {
				String instance = ents.getKey();
				InstanceConfig instanceConfig = ents.getValue();
				if (instanceConfig.getPipeParams().getWriteMechanism() != MECHANISM.AB) {
					setResponse(RESPONSE_STATUS.Success, "delete " + _instance + " Success!", null);
					return;
				}
				if (instance.equals(_instance) || instanceConfig.getAlias().equals(_instance)) {
					String[] L1seqs = EFMonitorUtil.getInstanceL1seqs(instance);
					if (L1seqs.length == 0) {
						L1seqs = new String[1];
						L1seqs[0] = GlobalParam.DEFAULT_RESOURCE_SEQ;
					}
					EFMonitorUtil.controlInstanceState(instance, STATUS.Stop, true);
					for (String L1seq : L1seqs) {
						String tags = Common.getResourceTag(instance, L1seq, GlobalParam.FLOW_TAG._DEFAULT.name(),
								false);
						WriterFlowSocket wfs = Resource.SOCKET_CENTER.getWriterSocket(
								Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getWriteTo(),
								instance, L1seq, tags);
						wfs.PREPARE(false, false);
						if (wfs.ISLINK()) {
							wfs.removeInstance(instance, GlobalParam.TASK_COORDER.getStoreIdFromSave(instance, L1seq, true));
							wfs.REALEASE(false, false);
						}
					}
					EFMonitorUtil.controlInstanceState(instance, STATUS.Ready, true);
				}
			}
			if (state) {
				setResponse(RESPONSE_STATUS.Success, "delete " + _instance + " Success!", null);
			} else {
				setResponse(RESPONSE_STATUS.CodeException, "delete " + _instance + " Failed!", null);
			}
		}
	}

	private boolean updateResourceXml(String resourcetype, JSONObject resourceData, boolean isDel) {
		try {
			String pondPath = GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("pond");
			byte[] resourceXml = EFDataStorer.getData(pondPath, false);
			String rname = resourceData.getString("name");
			SAXReader reader = new SAXReader();
			Document doc = reader.read(new ByteArrayInputStream(resourceXml));
			Element root = doc.getRootElement();
			Element content = root.element(resourcetype);
			List<?> socketlist = content.elements();
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
				Element newelement = content.addElement("socket");
				for (Map.Entry<String, Object> entry : resourceData.entrySet()) {
					Element element = newelement.addElement(entry.getKey());
					element.setText(entry.getValue().toString());
				}
			}
			EFDataStorer.setData(pondPath, Common.formatXml(doc));
		} catch (Exception e) {
			Common.LOG.error(e.getMessage());
			setResponse(RESPONSE_STATUS.CodeException, "save Resource Exception " + e.getMessage(), null);
			return false;
		}
		return true;
	}

	/**
	 * remove instance from system, stop all jobs and save to configure file.
	 * 
	 * @param instance
	 */
	private void removeInstance(String instance) {
		GlobalParam.INSTANCE_COORDER.removeInstance(instance);
		try {
			EFMonitorUtil.saveNodeConfig();
		} catch (Exception e) {
			setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
		}
	}
}
