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
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Base64.Decoder;

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
import org.elasticflow.model.EFRequest;
import org.elasticflow.model.InstructionTree;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFLoader;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.EFPipeUtil;
import org.elasticflow.util.PipeXMLUtil;
import org.elasticflow.util.SystemInfoUtil;
import org.elasticflow.util.instance.EFDataStorer;
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
			put("getresource", "getResource");
			put("updateresource", "updateResource");
			put("addresource", "addResource");
			put("removeresource", "removeResource");
			put("getnodeconfig", "getNodeConfig");
			put("setnodeconfig", "setNodeConfig");
			put("getstatus", "getStatus");
			put("getinstances", "getInstances");
			put("startsearcherservice", "startSearcherService");
			put("stopsearcherservice", "stopSearcherService");
			put("starthttpreaderserviceservice", "startHttpReaderServiceService");
			put("stopHttpreaderserviceservice", "stopHttpReaderServiceService");
			put("restartnode", "restartNode");
			put("loadhandler", "loadHandler");
			put("runcode", "runCode");
			// instance manage
			put("addinstance", "addInstance");
			put("cloneinstance", "cloneInstance");
			put("resetinstancestate", "resetInstanceState");
			put("getinstanceseqs", "getInstanceSeqs");
			put("reloadinstanceconfig", "reloadInstanceConfig");
			put("runnow", "runNow");
			put("addinstancetosystem", "addInstanceToSystem");
			put("stopinstance", "stopInstance");
			put("resumeinstance", "resumeInstance");
			put("removeinstance", "removeInstance");
			put("deleteinstancedata", "deleteInstanceData");
			put("getinstanceinfo", "getInstanceInfo");
			// pipe xml-config manage
			put("getinstancexml", "getInstanceXml");
			put("updateinstancexml", "updateInstanceXml");
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

	public void ac(Request rq,EFRequest RR, EFResponse RS) {
		try {
			if (RR.getParams().get("ac") != null && this.actions.containsKey(RR.getStringParam("ac").toLowerCase())) {
				Method m = NodeMonitor.class.getMethod(this.actions.get(RR.getStringParam("ac").toLowerCase()),
						Request.class,EFRequest.class);
				m.invoke(this, rq,RR);
				RS.setStatus(this.response_info, this.response_status);
				RS.setPayload(this.response_data);
			} else {
				RS.setPayload(this.actions);
				RS.setStatus("Example: /efm.doaction?ac=getStatus", RESPONSE_STATUS.ParameterErr);
			}
		} catch (Exception e) {
			RS.setStatus("Actions Exception!", RESPONSE_STATUS.CodeException);
			Common.LOG.error("ac " + RR.getParams().get("ac") + " Exception ", e);
		}
	}
	

	/**
	 * Be care full,this will remove all relative instance
	 * 
	 * @param rq
	 */
	public void removeResource(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "name,type")) {
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
	
	public void getResource(Request rq,EFRequest RR) {
		String pondPath = GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("pond");
		byte[] resourceXml = EFDataStorer.getData(pondPath, false);		
		setResponse(RESPONSE_STATUS.Success, "",new String(resourceXml));
	}
	
	public void updateResource(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "content")) {
			String pondPath = GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("pond");
			EFDataStorer.setData(pondPath,new String(decoder.decode(RR.getStringParam("content"))));
			setResponse(RESPONSE_STATUS.Success, "update resource!", null);
		}
	}

	/**
	 * @param socket resource configs json string
	 */
	public void addResource(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "socket")) {
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
	public void getNodeConfig(Request rq,EFRequest RR) {
		setResponse(RESPONSE_STATUS.Success, "", GlobalParam.StartConfig);
	}

	/**
	 * set node start configure parameters,will auto write into file.
	 * 
	 * @param k    property key
	 * @param v    property value
	 * @param type action type,set/remove
	 */
	public void setNodeConfig(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "content")) {
			try {
				String fpath = GlobalParam.configPath.replace("file:", "") + "/config.properties";
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
	public void restartNode(Request rq,EFRequest RR) {
		EFMonitorUtil.restartSystem();
		setResponse(RESPONSE_STATUS.CodeException, "current node is in restarting...", null);
	}

	/**
	 * Loading Java handler classes in real time only support no dependency handler
	 * like org.elasticflow.writerUnit.handler org.elasticflow.reader.handler
	 * org.elasticflow.searcher.handler
	 * 
	 * @param rq
	 */
	public void loadHandler(Request rq,EFRequest RR) {
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
	public void stopHttpReaderServiceService(Request rq,EFRequest RR) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
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
	public void startHttpReaderServiceService(Request rq,EFRequest RR) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
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
	public void stopSearcherService(Request rq,EFRequest RR) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
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
	public void startSearcherService(Request rq,EFRequest RR) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if ((service_level & 1) == 0) {
			service_level += 1;
			Resource.searcherService.start();
		}
		setResponse(RESPONSE_STATUS.Success, "Start Searcher Service Successed!", null);
	}

	/**
	 * get node environmental state.
	 * 
	 * @param rq
	 */
	public void getStatus(Request rq,EFRequest RR) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		JSONObject dt = new JSONObject();
		dt.put("NODE_TYPE", GlobalParam.StartConfig.getProperty("node_type"));
		dt.put("NODE_IP", GlobalParam.IP);
		dt.put("WRITE_BATCH", GlobalParam.WRITE_BATCH);
		dt.put("SERVICE_LEVEL", service_level);
		dt.put("STATUS", "running");
		dt.put("VERSION", GlobalParam.VERSION);
		dt.put("TASKS", Resource.tasks.size());
		dt.put("THREAD_POOL_SIZE", Resource.ThreadPools.getPoolSize());
		dt.put("SYS_THREAD_POOL_SIZE", GlobalParam.StartConfig.getProperty("sys_threadpool_size"));
		dt.put("THREAD_ACTIVE_COUNT", Resource.ThreadPools.getActiveCount());
		dt.put("DISTRIBUTE_RUN", GlobalParam.DISTRIBUTE_RUN);
		try {
			dt.put("CPU", SystemInfoUtil.getCpuUsage());
			dt.put("MEMORY", SystemInfoUtil.getMemUsage());
		} catch (Exception e) {
			Common.LOG.error("getStatus Exception ", e);
		}
		if (GlobalParam.DISTRIBUTE_RUN) {
			dt.put("SLAVES", GlobalParam.INSTANCE_COORDER.distributeCoorder().getNodeStatus());
		}
		setResponse(RESPONSE_STATUS.Success, null, dt);
	}

	/**
	 * Data source level delimited sequence
	 * 
	 * @param rq
	 */
	public void getInstanceSeqs(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance")) {
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
	public void resetInstanceState(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance")) {
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
				}
				setResponse(RESPONSE_STATUS.Success, RR.getStringParam("instance") + " reset Success!", null);
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.DataErr, RR.getStringParam("instance") + " not exists!", null);
			}
		}
	}

	/**
	 * get instance detail informations.
	 * 
	 * @param rq
	 */
	public void getInstanceInfo(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance")) {
			JSONObject JO = EFMonitorUtil.getInstanceInfo(RR.getStringParam("instance"), 7);
			if (JO.isEmpty()) {
				setResponse(RESPONSE_STATUS.DataErr, "instance not exits!", null);
			} else {
				setResponse(RESPONSE_STATUS.Success, null, JO);
			}
		}
	}
	
	public void getInstanceXml(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance")) {
			String xmlPath = GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance") + "/task.xml";
			byte[] datas = EFDataStorer.getData(xmlPath, false);		
			setResponse(RESPONSE_STATUS.Success, "",new String(datas));
		}
	}

	public void updateInstanceXml(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance,content")) {
			String xmlPath = GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance") + "/task.xml";
			EFDataStorer.setData(xmlPath, new String(decoder.decode(RR.getStringParam("content"))));
		}
	}

	/**
	 * eg instance=inscance_name&param.name=TransParam.multiThread&param.value=true
	 * 
	 * @param rq
	 */
	public void setInstancePipeConfig(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance,param.name,param.value")) {
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
					case "ReadParam":
						cls = tmp.getReadParams().getClass();
						obj = tmp.getReadParams();
						break;
					case "ComputeParam":
						cls = tmp.getComputeParams().getClass();
						obj = tmp.getComputeParams();
						break;
					case "WriteParam":
						cls = tmp.getWriterParams().getClass();
						obj = tmp.getWriterParams();
						break;
					}
					Common.setConfigObj(obj, cls, params[1], RR.getStringParam("param.value"));
					if (GlobalParam.DISTRIBUTE_RUN)
						GlobalParam.INSTANCE_COORDER.distributeCoorder().updateNodeConfigs(RR.getStringParam("instance"),
								params[0], params[1], RR.getStringParam("param.value"));
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
	public void getInstances(Request rq,EFRequest RR) {
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
	public void runCode(Request rq,EFRequest RR) throws EFException {
		if (RR.getStringParam("script") != null && RR.getStringParam("script").contains("Track.cpuFree")) {
			ArrayList<InstructionTree> Instructions = Common.compileCodes(RR.getStringParam("script"), CPU.getUUID());
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
	public void runNow(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance,jobtype")) {
			if (Resource.nodeConfig.getInstanceConfigs().containsKey(RR.getStringParam("instance"))
					&& Resource.nodeConfig.getInstanceConfigs().get(RR.getStringParam("instance")).openTrans()) {
				boolean state;
				if (GlobalParam.DISTRIBUTE_RUN) {
					state = GlobalParam.INSTANCE_COORDER.distributeCoorder()
							.runClusterInstanceNow(RR.getStringParam("instance"), RR.getStringParam("jobtype"), true);
				} else {
					state = Resource.FlOW_CENTER.runInstanceNow(RR.getStringParam("instance"), RR.getStringParam("jobtype"),
							true);
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
						"Writer " + RR.getStringParam("instance") + " job not open in this node!Run start faild!", null);
			}
		}
	}

	public void removeInstance(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance")) {
			removeInstance(RR.getStringParam("instance"));
			setResponse(RESPONSE_STATUS.Success, "Writer " + RR.getStringParam("instance") + " job have removed!", null);
		}
	}

	/**
	 * stop instance job.
	 * 
	 * @param rq
	 */
	public void stopInstance(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance,type")) {
			GlobalParam.INSTANCE_COORDER.stopInstance(RR.getStringParam("instance"), RR.getStringParam("type"));
			setResponse(RESPONSE_STATUS.Success, "Writer " + RR.getStringParam("instance") + " job stopped successfully!",
					null);
		}
	}

	/**
	 * resume instance job.
	 * 
	 * @param rq
	 */
	public void resumeInstance(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance,type")) {
			GlobalParam.INSTANCE_COORDER.resumeInstance(RR.getStringParam("instance"), RR.getStringParam("type"));
			setResponse(RESPONSE_STATUS.Success, "Writer " + RR.getStringParam("instance") + " job resumed successfully!",
					null);
		}
	}

	/**
	 * reload instance configure,auto rebuild instance in memory
	 * 
	 * @param rq instance=xx&reset=true|false reset true will recreate the instance
	 *           in java from instance configure.
	 */
	public void reloadInstanceConfig(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance")) {
			EFMonitorUtil.controlInstanceState(RR.getStringParam("instance"), STATUS.Stop, true);
			int type = Resource.nodeConfig.getInstanceConfigs().get(RR.getStringParam("instance")).getInstanceType();
			String instanceConfig = RR.getStringParam("instance");
			if (type > 0) {
				instanceConfig = RR.getStringParam("instance") + ":" + type;
			} else {
				if (!Resource.nodeConfig.getInstanceConfigs().containsKey(RR.getStringParam("instance")))
					setResponse(RESPONSE_STATUS.DataErr, RR.getStringParam("instance") + " not exists!", null);
			}
			Resource.FLOW_INFOS.remove(RR.getStringParam("instance"), JOB_TYPE.FULL.name());
			Resource.FLOW_INFOS.remove(RR.getStringParam("instance"), JOB_TYPE.INCREMENT.name());
			if (RR.getParams().get("reset") != null && RR.getStringParam("reset").equals("true")
					&& RR.getStringParam("instance").length() > 2) {
				Resource.nodeConfig.loadConfig(instanceConfig, true);
			} else {
				String alias = Resource.nodeConfig.getInstanceConfigs().get(RR.getStringParam("instance")).getAlias();
				Resource.nodeConfig.getSearchConfigs().remove(alias);
				Resource.nodeConfig.loadConfig(instanceConfig, false);
				EFPipeUtil.removeInstance(RR.getStringParam("instance"), true, true);
			}
			EFMonitorUtil.rebuildFlowGovern(instanceConfig, !GlobalParam.DISTRIBUTE_RUN);
			EFMonitorUtil.controlInstanceState(RR.getStringParam("instance"), STATUS.Ready, true);
			setResponse(RESPONSE_STATUS.Success, RR.getStringParam("instance") + " reload config Success!", null);
		}
	}
	
	public void addInstance(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance,content,level")) {
			String xmlPath = GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance") + "/task.xml";			
			try {
				EFDataStorer.createPath(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance"),false);
				EFDataStorer.setData(xmlPath, new String(decoder.decode(RR.getStringParam("content"))));
				EFMonitorUtil.addInstanceToSystem(RR.getStringParam("instance"),RR.getStringParam("level"));
				EFMonitorUtil.saveNodeConfig();
				setResponse(RESPONSE_STATUS.Success,
						RR.getStringParam("instance") + " save and push to node " + GlobalParam.IP + " success!", null);
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, e.getMessage(), null);
			}
		}
	}
	
	public void cloneInstance(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance,new_instance_name")) {
			EFFileUtil.copyFolder(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("instance"),
					GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("new_instance_name"));
			EFFileUtil.delFile(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("new_instance_name") + "/"+GlobalParam.JOB_FULLINFO_PATH);
			EFFileUtil.delFile(GlobalParam.INSTANCE_PATH + "/" + RR.getStringParam("new_instance_name") + "/"+GlobalParam.JOB_INCREMENTINFO_PATH);
			setResponse(RESPONSE_STATUS.Success, RR.getStringParam("new_instance_name") + " clone success!", null);
		}
	}

	/**
	 * add instance setting into system and add it to configure file also.
	 * 
	 * @param rq instance 
	 */
	public void addInstanceToSystem(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this,RR, "instance,level")) {			
			EFMonitorUtil.addInstanceToSystem(RR.getStringParam("instance"),RR.getStringParam("level"));
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
	 * delete Instance Data through alias or Instance data name
	 * 
	 * @param alias
	 * @return
	 */
	public void deleteInstanceData(Request rq,EFRequest RR) {
		if (EFMonitorUtil.checkParams(this, RR, "instance")) {
			String _instance = RR.getStringParam("instance");
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
						try {
							WriterFlowSocket wfs = Resource.SOCKET_CENTER.getWriterSocket(
									Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getWriteTo(),
									instance, L1seq, tags);
							wfs.PREPARE(false, false);
							if (wfs.ISLINK()) {
								wfs.removeInstance(instance,
										GlobalParam.TASK_COORDER.getStoreIdFromSave(instance, L1seq, true,false));
								wfs.REALEASE(false, false);
							}
						} catch (EFException e) {
							state = false;
							Common.LOG.error("delete Instance Data", e);
						}
					}
					EFMonitorUtil.controlInstanceState(instance, STATUS.Ready, true);
				}
			}
			if (state) {
				setResponse(RESPONSE_STATUS.Success, "delete " + _instance + " success!", null);
			} else {
				setResponse(RESPONSE_STATUS.CodeException, "delete " + _instance + " failed!", null);
			}
		}
	}

	private boolean updateResourceXml(JSONObject resourceData, boolean isDel) {
		try {
			String pondPath = GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("pond");
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
			Common.LOG.error(e.getMessage());
			setResponse(RESPONSE_STATUS.CodeException, "save resource exception " + e.getMessage(), null);
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
		if (GlobalParam.DISTRIBUTE_RUN) {
			GlobalParam.INSTANCE_COORDER.distributeCoorder().removeInstanceFromCluster(instance);
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
