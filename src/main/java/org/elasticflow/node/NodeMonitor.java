/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.node;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
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
import org.elasticflow.config.GlobalParam.INSTANCE_TYPE;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.MECHANISM;
import org.elasticflow.config.GlobalParam.RESOURCE_TYPE;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFSearchResponse;
import org.elasticflow.model.InstructionTree;
import org.elasticflow.param.pipe.InstructionParam;
import org.elasticflow.param.warehouse.WarehouseNosqlParam;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.param.warehouse.WarehouseSqlParam;
import org.elasticflow.piper.PipePump;
import org.elasticflow.reader.service.HttpReaderService;
import org.elasticflow.searcher.service.SearcherService;
import org.elasticflow.util.Common;
import org.elasticflow.util.ConfigStorer;
import org.elasticflow.util.EFLoader;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.util.SystemInfoUtil;
import org.elasticflow.writer.WriterFlowSocket;
import org.elasticflow.yarn.Resource;
import org.mortbay.jetty.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * * data-flow router maintain apis,default port
 * 8617,localhost:8617/search.doaction?ac=[actions]
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
@Component
@NotThreadSafe
public final class NodeMonitor {

	@Autowired
	private SearcherService SearcherService;

	@Autowired
	private HttpReaderService HttpReaderService;

	private RESPONSE_STATUS response_status;
	
	private Object response_info;

	private static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
		}
	};
 

	/**
	 * 
	 * @param status 0 faild 1 success
	 * @param info   response information
	 */
	public void setResponse(RESPONSE_STATUS status, Object info) { 
		this.response_status = status;
		this.response_info =info;
	}

	public void ac(Request rq,EFSearchResponse RS) {
		try {
			if (this.actions.contains(rq.getParameter("ac"))) {
				Method m = NodeMonitor.class.getMethod(rq.getParameter("ac"), Request.class);
				m.invoke(this, rq);
				RS.setStatus(this.response_info, this.response_status);
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
		if (rq.getParameter("name") != null && rq.getParameter("type") != null) {
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
				case NOSQL:
					wp = Resource.nodeConfig.getNoSqlWarehouse().get(name); 
					seqs = wp.getL1seq();
					if (seqs.length > 0) {
						for (String seq : seqs) {
							EFConnectionPool.clearPool(wp.getPoolName(seq));
						}
					} else {
						EFConnectionPool.clearPool(wp.getPoolName(null));
					}
					break;
					
				case SQL:
					wp = Resource.nodeConfig.getSqlWarehouse().get(name);
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
			JSONObject jsonObject =  new JSONObject();
			jsonObject.put("name",name);
			updateResourceXml(type.name(),jsonObject,true);
		} else {
			setResponse(RESPONSE_STATUS.ParameterErr, "Parameter not match!");
		}
	} 

	/**
	 * @param socket resource configs json string
	 */
	public void addResource(Request rq) {
		if (rq.getParameter("socket") != null && rq.getParameter("type") != null) {
			JSONObject jsonObject = JSON.parseObject(rq.getParameter("socket"));
			RESOURCE_TYPE type = RESOURCE_TYPE.valueOf(rq.getParameter("type").toUpperCase());
			Object o = null;
			Set<String> iter = jsonObject.keySet();
			try {
				switch (type) {
				case SQL:
					o = new WarehouseSqlParam();
					for (String key : iter) {
						Common.setConfigObj(o, WarehouseSqlParam.class, key, jsonObject.getString(key));
					} 
					break;
				case NOSQL:
					o = new WarehouseNosqlParam();
					for (String key : iter) {
						Common.setConfigObj(o, WarehouseNosqlParam.class, key, jsonObject.getString(key));
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
					setResponse(RESPONSE_STATUS.Success, "add Resource to node success!"); 
					updateResourceXml(type.name(), jsonObject,false);
				}
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, "add Resource to node Exception " + e.getMessage());
			}
		} else {
			setResponse(RESPONSE_STATUS.ParameterErr, "Parameter not match!");
		}
	}
	
	/**
	 * get node start run configure parameters.
	 * @param rq
	 */
	public void getNodeConfig(Request rq) {
		setResponse(RESPONSE_STATUS.Success, GlobalParam.StartConfig);
	}

	/**
	 * set node start configure parameters,will auto write into file.
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
				saveNodeConfig();
				setResponse(RESPONSE_STATUS.Success, "Config set success!");
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, "Config save Exception " + e.getMessage());
			}
		} else {
			setResponse(RESPONSE_STATUS.DataErr, "Config parameters k v or type not exists!");
		}
	}
	
	/**
	 * restart node
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
		setResponse(RESPONSE_STATUS.CodeException, "current node is in restarting...");
	}

	/**
	 * Loading Java handler classes in real time 
	 * only support no dependency handler like org.elasticflow.writerUnit.handler
	 * org.elasticflow.reader.handler org.elasticflow.searcher.handler
	 * 
	 * @param rq
	 */
	public void loadHandler(Request rq) {
		if (rq.getParameter("path") != null && rq.getParameter("name") != null) {
			try {
				new EFLoader(rq.getParameter("path")).loadClass(rq.getParameter("name"));
				setResponse(RESPONSE_STATUS.Success, "Load Handler success!");
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, "Load Handler Exception " + e.getMessage());
			}
		} else {
			setResponse(RESPONSE_STATUS.CodeException, "Parameters path not exists!");
		}

	}
	
	/**
	 * stop node http reader pipe service
	 * @param rq
	 */
	public void stopHttpReaderServiceService(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if ((service_level & 4) > 0) {
			service_level -= 4;
		}
		if (HttpReaderService.close()) {
			setResponse(RESPONSE_STATUS.Success, "Stop Searcher Service Successed!");
		} else {
			setResponse(RESPONSE_STATUS.CodeException, "Stop Searcher Service Failed!");
		}
	}
	
	/**
	 * start node http reader pipe service
	 * @param rq
	 */
	public void startHttpReaderServiceService(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if ((service_level & 4) == 0) {
			service_level += 4;
			HttpReaderService.start();
		}
		setResponse(RESPONSE_STATUS.Success, "Start Searcher Service Successed!");
	}
	
	/**
	 * stop node searcher service
	 * @param rq
	 */
	public void stopSearcherService(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if ((service_level & 1) > 0) {
			service_level -= 1;
		}
		if (SearcherService.close()) {
			setResponse(RESPONSE_STATUS.Success, "Stop Searcher Service Successed!");
		} else {
			setResponse(RESPONSE_STATUS.CodeException, "Stop Searcher Service Failed!");
		}
	}
	
	/**
	 * open node searcher service
	 * @param rq
	 */
	public void startSearcherService(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if ((service_level & 1) == 0) {
			service_level += 1;
			SearcherService.start();
		}
		setResponse(RESPONSE_STATUS.Success, "Start Searcher Service Successed!");
	}

	/**
	 * get node environmental state.
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
		setResponse(RESPONSE_STATUS.Success, dt);
	}

	/**
	 * Data source level delimited sequence
	 * @param rq
	 */
	public void getInstanceSeqs(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			try {
				String instance = rq.getParameter("instance");
				InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instance);
				WarehouseParam dataMap = Resource.nodeConfig.getNoSqlWarehouse()
						.get(instanceConfig.getPipeParams().getReadFrom());
				if (dataMap == null) {
					dataMap = Resource.nodeConfig.getSqlWarehouse().get(instanceConfig.getPipeParams().getReadFrom());
				}
				setResponse(RESPONSE_STATUS.Success, StringUtils.join(dataMap.getL1seq(), ","));
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, rq.getParameter("instance") + " not exists!");
			}
		} else {
			setResponse(RESPONSE_STATUS.ParameterErr, "Parameter not match!");
		}
	}
	
	/**
	 * reset Instance full and increment running state 
	 * @param rq
	 */
	public void resetInstanceState(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			try {
				String instance = rq.getParameter("instance");
				String val = "0";
				if (rq.getParameterMap().get("set_value") != null)
					val = rq.getParameter("set_value");
				String instanceName;
				String[] L1seqs = getInstanceL1seqs(instance);
				for (String L1seq : L1seqs) {
					instanceName = Common.getMainName(instance, L1seq);
					GlobalParam.SCAN_POSITION.get(instanceName).batchUpdateSeqPos(val);
					Common.saveTaskInfo(instance, L1seq, Common.getStoreId(instance, L1seq, false),
							GlobalParam.JOB_INCREMENTINFO_PATH);
				}
				setResponse(RESPONSE_STATUS.Success, rq.getParameter("instance") + " reset Success!");
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.DataErr, rq.getParameter("instance") + " not exists!");
			}
		} else {
			setResponse(RESPONSE_STATUS.ParameterErr, "Parameter not match!");
		}
	}
	
	/**
	 * get instance detail informations.
	 * @param rq
	 */
	public void getInstanceInfo(Request rq) {
		if (Resource.nodeConfig.getInstanceConfigs().containsKey(rq.getParameter("instance"))) {
			String instance = rq.getParameter("instance");
			JSONObject JO = new JSONObject();
			InstanceConfig config = Resource.nodeConfig.getInstanceConfigs().get(instance);
			if (Resource.nodeConfig.getNoSqlWarehouse().get(config.getPipeParams().getReadFrom()) != null) {
				String poolname = Resource.nodeConfig.getNoSqlWarehouse().get(config.getPipeParams().getReadFrom())
						.getPoolName(null);
				JO.put("Reader Pool Status", EFConnectionPool.getStatus(poolname));
			} else if (Resource.nodeConfig.getSqlWarehouse().get(config.getPipeParams().getReadFrom()) != null) {
				WarehouseSqlParam ws = Resource.nodeConfig.getSqlWarehouse().get(config.getPipeParams().getReadFrom());
				String poolname = "";
				if (ws.getL1seq() != null && ws.getL1seq().length > 0) {
					for (String seq : ws.getL1seq()) {
						poolname = Resource.nodeConfig.getSqlWarehouse().get(config.getPipeParams().getReadFrom())
								.getPoolName(seq);
						JO.put("Seq(" + seq + ") Reader Pool Status", EFConnectionPool.getStatus(poolname));
					}
				} else {
					poolname = Resource.nodeConfig.getSqlWarehouse().get(config.getPipeParams().getReadFrom())
							.getPoolName(null);
					JO.put("Reader Pool Status", EFConnectionPool.getStatus(poolname));
				}
			}
			
			String[] L1seqs = Common.getL1seqs(config,true);  
			for (String seq : L1seqs) {
				PipePump transDataFlow = Resource.SOCKET_CENTER.getPipePump(config.getName(), seq, false,GlobalParam.FLOW_TAG._DEFAULT.name());				if(seq=="") {
					JO.put("Reader Load ", transDataFlow.getReader().getLoad());
					JO.put("Reader Performance ", transDataFlow.getReader().getPerformance());
				}else {
					JO.put("Seq(" + seq + ") Reader Load ", transDataFlow.getReader().getLoad());
					JO.put("Seq(" + seq + ") Reader Performance ", transDataFlow.getReader().getPerformance());
				}
			}

			if (Resource.nodeConfig.getNoSqlWarehouse().get(config.getPipeParams().getWriteTo()) != null) {
				String poolname = Resource.nodeConfig.getNoSqlWarehouse().get(config.getPipeParams().getWriteTo())
						.getPoolName(null);
				JO.put("Writer Pool Status", EFConnectionPool.getStatus(poolname));
			} else if (Resource.nodeConfig.getSqlWarehouse().get(config.getPipeParams().getWriteTo()) != null) {
				String poolname = Resource.nodeConfig.getSqlWarehouse().get(config.getPipeParams().getWriteTo())
						.getPoolName(null);
				JO.put("Writer Pool Status", EFConnectionPool.getStatus(poolname));
			}

			if ((GlobalParam.SERVICE_LEVEL & 1) > 0) {
				String searchFrom = config.getPipeParams().getSearchFrom();
				String searcherInfo = "";
				if (config.getPipeParams().getWriteTo() != null
						&& config.getPipeParams().getWriteTo().equals(searchFrom)) {
					searcherInfo = "Searcher Pool (Share With Writer) Status";
				} else {
					searcherInfo = "Searcher Pool Status";
				}

				if (Resource.nodeConfig.getNoSqlWarehouse().get(searchFrom) != null) {
					String poolname = Resource.nodeConfig.getNoSqlWarehouse().get(searchFrom).getPoolName(null);
					JO.put(searcherInfo, EFConnectionPool.getStatus(poolname));
				} else {
					String poolname = Resource.nodeConfig.getSqlWarehouse().get(searchFrom).getPoolName(null);
					JO.put(searcherInfo, EFConnectionPool.getStatus(poolname));
				}
			}

			if (config.openTrans()) {
				WarehouseParam wsp = null;;
				wsp = Resource.nodeConfig.getSqlWarehouse().get(config.getPipeParams().getReadFrom());
				if(wsp==null)
					wsp = Resource.nodeConfig.getNoSqlWarehouse().get(config.getPipeParams().getReadFrom());
				if (wsp.getL1seq().length > 0) {
					StringBuilder sb = new StringBuilder();
					StringBuilder fullstate = new StringBuilder();
					for (String seq : wsp.getL1seq()) {
						String strs = GlobalParam.SCAN_POSITION.get(Common.getStoreName(instance, seq))
								.getPositionString();
						if (strs == null)
							continue;
						sb.append("\r\n;(" + seq + ") "
								+ GlobalParam.SCAN_POSITION.get(Common.getStoreName(instance, seq)).getStoreId() + ":");

						for (String str : strs.split(",")) {
							String update;
							String[] dstr = str.split(":");
							if (dstr[1].length() > 9 && dstr[1].matches("[0-9]+")) {
								update = dstr[0] + ":"
										+ (SDF.format(
												dstr[1].length() < 12 ? Long.valueOf(dstr[1] + "000") :  Long.valueOf(dstr[1])))
										+ " (" + dstr[1] + ")";
							} else {
								update = str;
							}
							sb.append(", ");
							sb.append(update);
						}
						fullstate.append(seq + ":" + Common.getFullStartInfo(instance, seq) + "; ");
					}
					JO.put("增量存储状态", sb);
					JO.put("全量存储状态", fullstate);
				} else {
					String strs = GlobalParam.SCAN_POSITION.get(Common.getStoreName(instance, null))
							.getPositionString();
					if(strs.length()>0) {
						StringBuilder stateStr = new StringBuilder();
						if (strs.split(",").length > 0) {
							for (String tm : strs.split(",")) {
								String[] dstr = tm.split(":");
								if (dstr[1].length() > 9 && dstr[1].matches("[0-9]+")) {
									stateStr.append(dstr[0] + ":"
											+ SDF.format(tm.length() < 12 ?  Long.valueOf(dstr[1] + "000") : Long.valueOf(dstr[1])));
									stateStr.append(" (").append(tm).append(")");
								} else {
									stateStr.append(tm);
								}
								stateStr.append(", ");
							}
						}
						JO.put("增量存储状态", GlobalParam.SCAN_POSITION.get(Common.getStoreName(instance, null)).getStoreId()
								+ ":" + stateStr.toString());
					}
					JO.put("全量存储状态", Common.getFullStartInfo(instance, null));
				}
				if (!Resource.FLOW_INFOS.containsKey(instance, JOB_TYPE.FULL.name())
						|| Resource.FLOW_INFOS.get(instance, JOB_TYPE.FULL.name()).size() == 0) {
					JO.put("全量运行状态", "full:null");
				} else {
					JO.put("全量运行状态", "full:" + Resource.FLOW_INFOS.get(instance, JOB_TYPE.FULL.name()));
				}
				if (!Resource.FLOW_INFOS.containsKey(instance, JOB_TYPE.INCREMENT.name())
						|| Resource.FLOW_INFOS.get(instance, JOB_TYPE.INCREMENT.name()).size() == 0) {
					JO.put("增量运行状态", "increment:null");
				} else {
					JO.put("增量运行状态", "increment:" + Resource.FLOW_INFOS.get(instance, JOB_TYPE.INCREMENT.name()));
				}
				JO.put("增量线程状态", threadStateInfo(instance, GlobalParam.JOB_TYPE.INCREMENT));
				JO.put("全量线程状态", threadStateInfo(instance, GlobalParam.JOB_TYPE.FULL));
			}
			setResponse(RESPONSE_STATUS.Success, JO);
		} else {
			setResponse(RESPONSE_STATUS.DataErr, "instance not exits!");
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
			instance.put("InstanceType", this.getInstanceType(config.getInstanceType()));

			if (rs.containsKey(config.getAlias())) {
				rs.get(config.getAlias()).add(instance);
				rs.put(config.getAlias(), rs.get(config.getAlias()));
			} else {
				ArrayList<JSONObject> tmp = new ArrayList<JSONObject>();
				tmp.add(instance);
				rs.put(config.getAlias(), tmp);
			}
		}
		setResponse(RESPONSE_STATUS.Success, rs);
	}
	
	/**
	 * run ElasticFlow CPU instruction program.
	 * @param rq
	 */
	public void runCode(Request rq) {
		if (rq.getParameter("script") != null && rq.getParameter("script").contains("Track.cpuFree")) {
			ArrayList<InstructionTree> Instructions = Common.compileCodes(rq.getParameter("script"), CPU.getUUID());
			for (InstructionTree Instruction : Instructions) {
				Instruction.depthRun(Instruction.getRoot());
			}
			setResponse(RESPONSE_STATUS.Success, "code run success!");
		} else {
			setResponse(RESPONSE_STATUS.DataErr, "script not set or script grammer is not correct!");
		}
	}
	
	/**
	 * Perform the instance task immediately  
	 * @param rq
	 */
	public void runNow(Request rq) {
		if (rq.getParameter("instance") != null && rq.getParameter("jobtype") != null) {
			if (Resource.nodeConfig.getInstanceConfigs().containsKey(rq.getParameter("instance"))
					&& Resource.nodeConfig.getInstanceConfigs().get(rq.getParameter("instance")).openTrans()) {
				boolean state = Resource.FlOW_CENTER.runInstanceNow(rq.getParameter("instance"),
						rq.getParameter("jobtype"), true);
				if (state) {
					setResponse(RESPONSE_STATUS.Success, "Writer " + rq.getParameter("instance") + " job has been started now!");
				} else {
					setResponse(RESPONSE_STATUS.DataErr, "Writer " + rq.getParameter("instance")
							+ " job not exists or run failed or had been stated!");
				}
			} else {
				setResponse(RESPONSE_STATUS.DataErr, "Writer " + rq.getParameter("instance") + " job not open in this node!Run start faild!");
			}
		} else {
			setResponse(RESPONSE_STATUS.DataErr, "Writer " + rq.getParameter("instance")
					+ " job started now error,instance and jobtype parameter not both set!");
		}
	}

	public void removeInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			removeInstance(rq.getParameter("instance"));
			setResponse(RESPONSE_STATUS.Success, "Writer " + rq.getParameter("instance") + " job have removed!");
		} else {
			setResponse(RESPONSE_STATUS.DataErr, "Writer " + rq.getParameter("instance") + " remove error,instance parameter not set!");
		}
	}
	
	/**
	 * stop instance job.
	 * @param rq
	 */
	public void stopInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			if (rq.getParameter("type").toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
				controlThreadState(rq.getParameter("instance"), STATUS.Stop, false);
			} else {
				controlThreadState(rq.getParameter("instance"), STATUS.Stop, true);
			}
			setResponse(RESPONSE_STATUS.Success, "Writer " + rq.getParameter("instance") + " job have stopped!");
		} else {
			setResponse(RESPONSE_STATUS.DataErr, "Writer " + rq.getParameter("instance") + " stop error,index parameter not set!");
		}
	}
	
	/**
	 * resume instance job.
	 * @param rq
	 */
	public void resumeInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			if (rq.getParameter("type").toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
				controlThreadState(rq.getParameter("instance"), STATUS.Ready, false);
			} else {
				controlThreadState(rq.getParameter("instance"), STATUS.Ready, true);
			}
			setResponse(RESPONSE_STATUS.Success, "Writer " + rq.getParameter("instance") + " job have resumed!");
		} else {
			setResponse(RESPONSE_STATUS.DataErr, "Writer " + rq.getParameter("instance") + " resume error,index parameter not set!");
		}
	}
	
	/**
	 * reload instance configure,auto rebuild instance in memory
	 * @param rq instance=xx&reset=true|false 
	 * reset true will recreate the instance in java from instance configure.
	 */
	public void reloadInstanceConfig(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			controlThreadState(rq.getParameter("instance"), STATUS.Stop, true);
			int type = Resource.nodeConfig.getInstanceConfigs().get(rq.getParameter("instance")).getInstanceType();
			String instanceConfig = rq.getParameter("instance");
			if (type > 0) {
				instanceConfig = rq.getParameter("instance") + ":" + type;
			} else {
				if (!Resource.nodeConfig.getInstanceConfigs().containsKey(rq.getParameter("instance")))
					setResponse(RESPONSE_STATUS.DataErr, rq.getParameter("instance") + " not exists!");
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
			rebuildFlowGovern(instanceConfig);
			controlThreadState(rq.getParameter("instance"), STATUS.Ready, true);
			setResponse(RESPONSE_STATUS.Success, rq.getParameter("instance") + " reload Config Success!");
		} else {
			setResponse(RESPONSE_STATUS.DataErr, rq.getParameter("instance") + " not exists!");
		}
	}

	/**
	 * add instance into system and add to configure also.
	 * @param rq instance parameter example,instanceName:1
	 */
	public void addInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			Resource.nodeConfig.loadConfig(rq.getParameter("instance"), false);
			String tmp[] = rq.getParameter("instance").split(":");
			String instanceName = rq.getParameter("instance");
			if (tmp.length > 1)
				instanceName = tmp[0];
			InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instanceName);
			if (instanceConfig.checkStatus())
				EFNodeUtil.initParams(instanceConfig);
			rebuildFlowGovern(rq.getParameter("instance"));
			GlobalParam.StartConfig.setProperty("instances",
					(GlobalParam.StartConfig.getProperty("instances") + "," + rq.getParameter("instance")).replace(",,",
							","));
			try {
				saveNodeConfig();
				setResponse(RESPONSE_STATUS.Success, instanceName + " add to node " + GlobalParam.IP + " Success!");
			} catch (Exception e) {
				setResponse(RESPONSE_STATUS.CodeException, e.getMessage());
			}
		} else {
			setResponse(RESPONSE_STATUS.ParameterErr, "Parameter not match!");
		}
	}

	/**
	 * delete Instance Data through alias or Instance data name
	 * 
	 * @param alias
	 * @return
	 */
	public void deleteInstanceData(Request rq) {
		if (rq.getParameter("instance") != null) {
			String _instance = rq.getParameter("instance");
			Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
			boolean state = true;
			for (Map.Entry<String, InstanceConfig> ents : configMap.entrySet()) {
				String instance = ents.getKey();
				InstanceConfig instanceConfig = ents.getValue();
				if (instanceConfig.getPipeParams().getWriteMechanism() != MECHANISM.AB) {
					setResponse(RESPONSE_STATUS.Success, "delete " + _instance + " Success!");
					return;
				}
				if (instance.equals(_instance) || instanceConfig.getAlias().equals(_instance)) {
					String[] L1seqs = getInstanceL1seqs(instance);
					if (L1seqs.length == 0) {
						L1seqs = new String[1];
						L1seqs[0] = GlobalParam.DEFAULT_RESOURCE_SEQ;
					}
					controlThreadState(instance, STATUS.Stop, true);
					for (String L1seq : L1seqs) {
						String tags = Common.getResourceTag(instance, L1seq, GlobalParam.FLOW_TAG._DEFAULT.name(),
								false);
						WriterFlowSocket wfs = Resource.SOCKET_CENTER.getWriterSocket(
								Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getWriteTo(),
								instance, L1seq, tags);
						wfs.PREPARE(false, false);
						if (wfs.ISLINK()) {
							wfs.removeInstance(instance, Common.getStoreId(instance, L1seq, true));
							wfs.REALEASE(false, false);
						}
					}
					controlThreadState(instance, STATUS.Ready, true);
				}
			}
			if (state) {
				setResponse(RESPONSE_STATUS.Success, "delete " + _instance + " Success!");
			} else {
				setResponse(RESPONSE_STATUS.CodeException, "delete " + _instance + " Failed!");
			}
		} else {
			setResponse(RESPONSE_STATUS.ParameterErr, "Parameter not match!");
		}
	} 
	
	private boolean updateResourceXml(String resourcetype,JSONObject resourceData,boolean isDel) {  
        try {
        	String pondPath = GlobalParam.CONFIG_PATH + "/" + GlobalParam.StartConfig.getProperty("pond");
        	byte[] resourceXml = ConfigStorer.getData(pondPath,false);
            String rname = resourceData.getString("name");
            SAXReader reader = new SAXReader();
            Document doc = reader.read(new ByteArrayInputStream(resourceXml));
            Element root = doc.getRootElement(); 
            Element content = root.element(resourcetype);
            List<?> socketlist = content.elements();
            boolean isExist = false;
            for (Iterator<?> it = socketlist.iterator(); it.hasNext();) {
                Element socket = (Element) it.next(); 
                if(rname.equals(socket.element("name").getTextTrim())){
                	if (isDel) {
                		socketlist.remove(socket);
                		break;
                	}
                	isExist = true;
                    List<?> itemlist = socket.elements();
                    for (Iterator<?> sitem = itemlist.iterator(); sitem.hasNext();) {
                        Element socketContent = (Element) sitem.next();
                        if(resourceData.getString(socketContent.getName())!=null&&resourceData.getString(socketContent.getName())!=""){
							if (socketContent
									.getText() != resourceData.getString(socketContent.getName())
                                    && !socketContent.getText().equals(resourceData.getString(socketContent.getName()))){
								socketContent.setText(resourceData.getString(socketContent.getName()));
                            }
							resourceData.remove(socketContent.getName());

                        }else if(resourceData.getString(socketContent.getName())==null || resourceData.getString(socketContent.getName())==""){
                            socket.remove(socketContent);
                            resourceData.remove(socketContent.getName());
                        }
                    }
                    if(resourceData.size()>0){
                        for (Map.Entry<String, Object> entry : resourceData.entrySet()) {
                            Element socketinfo = socket.addElement(entry.getKey());
                            socketinfo.setText(entry.getValue().toString());
                        }
                    }
                }else{
                    continue;
                }
            }
            if(!isExist && !isDel){
                Element newelement = content.addElement("socket");
                for (Map.Entry<String, Object> entry : resourceData.entrySet()) {
                    Element element = newelement.addElement(entry.getKey());
                    element.setText(entry.getValue().toString());
                }
            }  
            ConfigStorer.setData(pondPath, Common.formatXml(doc)); 
        } catch (Exception e) {
        	Common.LOG.error(e.getMessage());
			setResponse(RESPONSE_STATUS.CodeException, "save Resource Exception " + e.getMessage());
			return false;
		} 
        return true;
    }
	
	/**
	 * remove instance from system, stop all jobs and save to configure file.
	 * @param instance
	 */
	private void removeInstance(String instance) {
		controlThreadState(instance, STATUS.Stop, true);
		if (Resource.nodeConfig.getInstanceConfigs().get(instance).getInstanceType() > 0) {
			Resource.FLOW_INFOS.remove(instance, JOB_TYPE.FULL.name());
			Resource.FLOW_INFOS.remove(instance, JOB_TYPE.INCREMENT.name());
		}
		Resource.nodeConfig.getInstanceConfigs().remove(instance);
		Resource.FlOW_CENTER.removeInstance(instance, true, true);
		String tmp = "";
		for (String str : GlobalParam.StartConfig.getProperty("instances").split(",")) {
			String[] s = str.split(":");
			if (s[0].equals(instance))
				continue;
			tmp += str + ",";
		}
		GlobalParam.StartConfig.setProperty("instances", tmp);
		try {
			saveNodeConfig();
		} catch (Exception e) {
			setResponse(RESPONSE_STATUS.CodeException, e.getMessage());
		}
	}

	private String getInstanceType(int type) {
		if (type > 0) {
			String res = "";
			if ((type & INSTANCE_TYPE.Trans.getVal()) > 0)
				res += INSTANCE_TYPE.Trans.name() + ",";
			if ((type & INSTANCE_TYPE.WithCompute.getVal()) > 0)
				res += INSTANCE_TYPE.WithCompute.name();
			return res;
		} else {
			return INSTANCE_TYPE.Blank.name();
		}
	}

	/**
	 * control current run thread, prevent error data write
	 * 
	 * @param instance    multi-instances seperate with ","
	 * @param state
	 * @param isIncrement control thread type
	 */
	private void controlThreadState(String instance, STATUS state, boolean isIncrement) {
		if ((GlobalParam.SERVICE_LEVEL & 6) == 0) {
			return;
		}
		JOB_TYPE controlType = GlobalParam.JOB_TYPE.FULL;
		if (isIncrement)
			controlType = GlobalParam.JOB_TYPE.INCREMENT;

		for (String inst : instance.split(",")) {
			Common.LOG.info("Instance " + inst + " waitting set state " + state + " ...");
			int waittime = 0;
			String[] seqs = getInstanceL1seqs(instance);
			for (String seq : seqs) {
				if (Common.checkFlowStatus(inst, seq, controlType, STATUS.Running)) {
					Common.setFlowStatus(inst, seq, controlType.name(), STATUS.Blank, STATUS.Termination,true);
					while (!Common.checkFlowStatus(inst, seq, controlType, STATUS.Ready)) {
						try {
							waittime++;
							Thread.sleep(300);
							if (waittime > 200) {
								break;
							}
						} catch (InterruptedException e) {
							Common.LOG.error("currentThreadState InterruptedException", e);
						}
					}
				}
				Common.setFlowStatus(inst, seq, controlType.name(), STATUS.Blank, STATUS.Termination,true);
				if (Common.setFlowStatus(inst, seq, controlType.name(), STATUS.Termination, state,true)) {
					Common.LOG.info("Instance " + inst + " success set state " + state);
				} else {
					Common.LOG.info("Instance " + inst + " fail set state " + state);
				}
			}
		}
	}

	private String threadStateInfo(String instance, JOB_TYPE type) {
		String[] seqs = getInstanceL1seqs(instance);
		StringBuilder sb = new StringBuilder();
		for (String seq : seqs) {
			sb.append(seq.length() == 0 ? "MAIN " : seq + ":");
			if (Common.checkFlowStatus(instance, seq, type, STATUS.Stop))
				sb.append("Stop,");
			if (Common.checkFlowStatus(instance, seq, type, STATUS.Ready))
				sb.append("Ready,");
			if (Common.checkFlowStatus(instance, seq, type, STATUS.Running))
				sb.append("Running,");
			if (Common.checkFlowStatus(instance, seq, type, STATUS.Termination))
				sb.append("Termination,");
			sb.append(" ;");
		}
		return sb.toString();
	}

	private String[] getInstanceL1seqs(String instance) {
		InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instance);
		WarehouseParam dataMap = Resource.nodeConfig.getNoSqlWarehouse()
				.get(instanceConfig.getPipeParams().getReadFrom());
		if (dataMap == null) {
			dataMap = Resource.nodeConfig.getSqlWarehouse().get(instanceConfig.getPipeParams().getReadFrom());
		}
		String[] seqs;
		if (dataMap == null) {
			seqs = new String[] {};
		} else {
			seqs = dataMap.getL1seq();
		}

		if (seqs.length == 0) {
			seqs = new String[1];
			seqs[0] = GlobalParam.DEFAULT_RESOURCE_SEQ;
		}
		return seqs;
	}

	private void rebuildFlowGovern(String index) {
		for (String inst : index.split(",")) {
			String[] strs = inst.split(":");
			if (strs.length < 1)
				continue;
			Resource.FlOW_CENTER.addFlowGovern(strs[0], Resource.nodeConfig.getInstanceConfigs().get(strs[0]), true);
		}
	}

	private void saveNodeConfig() throws Exception {
		OutputStream os = null;
		os = new FileOutputStream(GlobalParam.configPath.replace("file:", "") + "/config.properties");
		GlobalParam.StartConfig.store(os, "Auto Save Config with no format,BeCarefull!");
	}
}
