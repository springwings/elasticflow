package org.elasticflow.util;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.config.GlobalParam.INSTANCE_TYPE;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.model.EFRequest;
import org.elasticflow.node.NodeMonitor;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.piper.PipePump;
import org.elasticflow.yarn.Resource;

import com.alibaba.fastjson.JSONObject;

/**
 * @author chengwen
 * @version 3.0
 * @date 2018-10-25 09:08
 */

public class EFMonitorUtil {
	
	public static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static boolean checkParams(NodeMonitor obj,EFRequest rq,String checkParams){ 
		for(String param:checkParams.split(",")) {
			if(rq.getParam(param)==null || rq.getStringParam(param).strip().equals("")) {
				obj.setResponse(RESPONSE_STATUS.ParameterErr, checkParams+" parameters may be missing!", null);
				return false;
			}
		} 
		return true;
	}	
	 

	public static void saveNodeConfig() throws Exception {
		OutputStream os = null;
		os = new FileOutputStream(GlobalParam.configPath.replace("file:", "") + "/config.properties");
		GlobalParam.StartConfig.store(os,null);
	}
	
	/**
	 * add instance to properties file
	 * @param instanceSetting
	 */
	public static void addInstanceToConfig(String instanceSetting) {
		String[] instanceSets = instanceSetting.split(":");
		boolean isset = false;
		String tmp = "";
		for (String str : GlobalParam.StartConfig.getProperty("instances").split(",")) {
			String[] s = str.split(":");
			if (s[0].equals(instanceSets[0])) {
				tmp += ","+instanceSetting;	
				isset = true;
			}else {
				tmp += ","+str;
			}			
		}
		if(!isset)
			tmp += ","+instanceSetting;
		
		if(tmp.length()>2)
			tmp = tmp.substring(1); 
		GlobalParam.StartConfig.setProperty("instances", tmp);
	}
	
	public static void addInstanceToSystem(String instance,String level) {
		String instanceString = instance+":"+level;		
		if (GlobalParam.DISTRIBUTE_RUN) {
			GlobalParam.INSTANCE_COORDER.addInstance(instanceString,false);
			GlobalParam.INSTANCE_COORDER.distributeCoorder().pushInstanceToCluster(instanceString);
		}else {
			GlobalParam.INSTANCE_COORDER.addInstance(instanceString,true);
		}
		addInstanceToConfig(instanceString);
	}
	
	/**
	 * remove instance from properties file
	 * 
	 * @param instance
	 */
	public static void removeConfigInstance(String instance) {
		String tmp = "";
		for (String str : GlobalParam.StartConfig.getProperty("instances").split(",")) {
			String[] s = str.split(":");
			if (s[0].equals(instance))
				continue;
			tmp += str + ",";
		}
		GlobalParam.StartConfig.setProperty("instances", tmp);
	}

	
	public static void rebuildFlowGovern(String instanceSettting,boolean createSchedule) {
		for (String inst : instanceSettting.split(",")) {
			String[] strs = inst.split(":");
			if (strs.length < 1)
				continue;
			Resource.FlOW_CENTER.addFlowGovern(strs[0], Resource.nodeConfig.getInstanceConfigs().get(strs[0]), true,createSchedule);
		}
	}
	
	public static void cleanAllInstance(boolean waitComplete) {
		Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
		for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) {
			GlobalParam.INSTANCE_COORDER.removeInstance(entry.getKey(),waitComplete);
		}
	}
	
	public static String[] getInstanceL1seqs(String instance) {
		InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instance);
		WarehouseParam dataMap = Resource.nodeConfig.getWarehouse()
				.get(instanceConfig.getPipeParams().getReadFrom());
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
	

	public static String getInstanceType(int type) {
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
	
	public static JSONObject threadStateInfo(String instance, JOB_TYPE type) {
		String[] seqs = EFMonitorUtil.getInstanceL1seqs(instance);
		JSONObject info = new JSONObject();
		for (String seq : seqs) {
			String state = "";
			if (GlobalParam.TASK_COORDER.checkFlowStatus(instance, seq, type, STATUS.Stop))
				state = "stop";
			if (GlobalParam.TASK_COORDER.checkFlowStatus(instance, seq, type, STATUS.Ready))				
				state = "Ready";
			if (GlobalParam.TASK_COORDER.checkFlowStatus(instance, seq, type, STATUS.Running))
				state = "Running";
			if (GlobalParam.TASK_COORDER.checkFlowStatus(instance, seq, type, STATUS.Termination))
				state = "Termination";
			info.put(seq.length() == 0 ? "MAIN " : seq, state);
		}
		return info;
	}
	

	/**
	 * control Instance current run thread, prevent error data write
	 * 
	 * @param instance    multi-instances seperate with ","
	 * @param state
	 * @param isIncrement control thread type
	 */
	public static void controlInstanceState(String instance, STATUS state, boolean isIncrement) {
		InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instance);
		if ((GlobalParam.SERVICE_LEVEL & 6) == 0) {
			return;
		}
		JOB_TYPE controlType;
		if (isIncrement) {
			if(instanceConfig.getPipeParams().getDeltaCron() == null)
				return;
			controlType = GlobalParam.JOB_TYPE.INCREMENT;
		}else {
			if(instanceConfig.getPipeParams().getFullCron() == null)
				return;
			controlType = GlobalParam.JOB_TYPE.FULL;
		}
			

		for (String inst : instance.split(",")) { 
			int waittime = 0;
			String[] seqs = EFMonitorUtil.getInstanceL1seqs(instance);
			Common.LOG.info("Instance {} seq nums {},job type {},waitting set state {} ...",inst,seqs.length,controlType.name(),state);
			for (String L1seq : seqs) {
				if (GlobalParam.TASK_COORDER.checkFlowStatus(inst, L1seq, controlType, STATUS.Running)) {
					GlobalParam.TASK_COORDER.setFlowStatus(inst, L1seq, controlType.name(), STATUS.Blank, STATUS.Termination, true);
					while (!GlobalParam.TASK_COORDER.checkFlowStatus(inst, L1seq, controlType, STATUS.Ready)) {
						try {
							waittime++;
							Thread.sleep(100);
							if (waittime > 200) {
								break;
							}
						} catch (InterruptedException e) {
							Common.LOG.error("currentThreadState InterruptedException", e);
						}
					}
				}
				GlobalParam.TASK_COORDER.setFlowStatus(inst, L1seq, controlType.name(), STATUS.Blank, STATUS.Termination, true);
				if (GlobalParam.TASK_COORDER.setFlowStatus(inst, L1seq, controlType.name(), STATUS.Termination, state, true)) {
					Common.LOG.info("Instance {} L1seq {},job type {} success set state {}.",inst,(L1seq.length()==0?GlobalParam.DEFAULT_SEQ:L1seq),controlType.name(),state);
				} else {
					Common.LOG.info("Instance {} L1seq {},job type {} fail set state {}.",inst,(L1seq.length()==0?GlobalParam.DEFAULT_SEQ:L1seq),controlType.name(),state);
				}
			}
		}
	}
	
	public static String getConnectionStatus(String instance,String poolName) {
		if(GlobalParam.DISTRIBUTE_RUN) {
			return GlobalParam.INSTANCE_COORDER.distributeCoorder().getConnectionStatus(instance,poolName);
		}else {
			return EFConnectionPool.getStatus(poolName);
		}
	}
	
	/**
	 * get Pipe End Status
	 * @param instance
	 * @param L1seq
	 * @return
	 */
	public static JSONObject getPipeEndStatus(String instance, String L1seq) {
		JSONObject res = new JSONObject();  
		res.put(END_TYPE.reader.name(), "Not started!");
		res.put(END_TYPE.computer.name(), "Not started!");
		res.put(END_TYPE.computer.name(), "Not started!");
		res.put("status", "offline");
		PipePump pipePump = null;
		try {
			pipePump = Resource.SOCKET_CENTER.getPipePump(instance, L1seq, false,
					GlobalParam.FLOW_TAG._DEFAULT.name());
		} catch (EFException e) {
			Common.LOG.error("",e);
		} 
		if(pipePump!=null) {
			InstanceConfig config = Resource.nodeConfig.getInstanceConfigs().get(instance);
			if ((config.getInstanceType() & INSTANCE_TYPE.Trans.getVal()) > 0) { 
				res.put(END_TYPE.reader.name(), pipePump.getReader().flowState.get());
				res.put(END_TYPE.writer.name(), pipePump.getWriter().flowState.get());
			}		
			if ((config.getInstanceType() & INSTANCE_TYPE.WithCompute.getVal()) > 0) {	
				res.put(END_TYPE.computer.name(), pipePump.getComputer().flowState.get());
			}
			res.put("status", "online");
			res.put("nodeIP", GlobalParam.IP);
			res.put("nodeID", GlobalParam.NODEID);
		}
		return res;
	}
	
/**
 * get instance detail informations.
 * @param instance
 * @param type 
 * 1 pool status ++
 * 2 Performance status ++
 * 4 task status ++
 * @return
 */
	public static JSONObject getInstanceInfo(String instance,int type) {
		JSONObject JO = new JSONObject();
		if (Resource.nodeConfig.getInstanceConfigs().containsKey(instance)) {			
			InstanceConfig config = Resource.nodeConfig.getInstanceConfigs().get(instance);
			JSONObject Reader = new JSONObject();
			JSONObject Writer = new JSONObject();
			JSONObject nodeInfo = new JSONObject();
			JSONObject Computer = new JSONObject();
			JSONObject Searcher = new JSONObject();
			JSONObject Task = new JSONObject();
			if((type&1)>0) {
				if (Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom()) != null) {	
					WarehouseParam wp = Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom());
					JSONObject poolstatus = new JSONObject();
					for(String seq:wp.getL1seq()) {
						poolstatus.put(seq, getConnectionStatus(instance,wp.getPoolName(seq)));
					}					
					Reader.put("pool_status", poolstatus);
				}
				

				if (Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getWriteTo()) != null) {
					WarehouseParam wp = Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getWriteTo());
					JSONObject poolstatus = new JSONObject();
					for(String seq:wp.getL1seq()) {
						poolstatus.put(seq, getConnectionStatus(instance,wp.getPoolName(seq)));
					}						
					Writer.put("pool_status", poolstatus);
				}

				if ((GlobalParam.SERVICE_LEVEL & 1) > 0) {
					String searchFrom = config.getPipeParams().getSearchFrom();
					String searcherInfo = "";
					if (config.getPipeParams().getWriteTo() != null
							&& config.getPipeParams().getWriteTo().equals(searchFrom)) {
						searcherInfo = "pool_status_(default_is_writer)";
					} else {
						searcherInfo = "pool_status";
					}					
					String poolname = Resource.nodeConfig.getWarehouse().get(searchFrom).getPoolName(GlobalParam.DEFAULT_RESOURCE_SEQ);
					Searcher.put(searcherInfo, getConnectionStatus(instance,poolname));					
				}
			}
			
			if((type&2)>0) {
				String[] L1seqs = Common.getL1seqs(config);
				for (String L1seq : L1seqs) {	 
					String appendPipe = "";
					if (L1seq != "") {
						appendPipe = "L1seq(" + L1seq + ")_";
					}
					Task.put(appendPipe + "breaker_is_on", Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).breaker.isOn());
					Task.put(appendPipe + "valve_turn_level", Resource.tasks.get(Common.getInstanceRunId(instance, L1seq)).valve.getTurnLevel());
					JSONObject tmp;
					if(GlobalParam.DISTRIBUTE_RUN) {
						tmp = GlobalParam.INSTANCE_COORDER.distributeCoorder().getPipeEndStatus(config.getInstanceID(), L1seq);						
					} else {
						tmp = getPipeEndStatus(config.getInstanceID(), L1seq);
					} 
					Searcher.put(appendPipe + "flow_state", tmp.get(END_TYPE.searcher.name()));
					Reader.put(appendPipe + "flow_state", tmp.get(END_TYPE.reader.name()));
					if ((config.getInstanceType() & INSTANCE_TYPE.WithCompute.getVal()) > 0) {
						Computer.put(appendPipe + "flow_state", tmp.get(END_TYPE.computer.name()));
					}
					if ((config.getInstanceType() & INSTANCE_TYPE.Trans.getVal()) > 0) {
						Writer.put(appendPipe + "flow_state", tmp.get(END_TYPE.writer.name()));
					}
					nodeInfo.put(appendPipe+"node_ip", tmp.get("nodeIP"));
					nodeInfo.put(appendPipe+"node_id", tmp.get("nodeID")); 
					nodeInfo.put(appendPipe+"pipe_size", 
							Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getReadPageSize());
					nodeInfo.put(appendPipe+"status", tmp.get("status"));
				}
			}			
				
			if((type&4)>0) {
				if (config.openTrans()) { 
					Task.put("incremental_storage_status", GlobalParam.TASK_COORDER.getInstanceScanDatas(instance,false));
					Task.put("full_storage_status",GlobalParam.TASK_COORDER.getInstanceScanDatas(instance,true));
					Task.put("full_progress", new JSONObject());
					if (!Resource.FLOW_INFOS.containsKey(instance, JOB_TYPE.FULL.name())
							|| Resource.FLOW_INFOS.get(instance, JOB_TYPE.FULL.name()).size() == 0) {
						Task.getJSONObject("full_progress").put("full","none");
					} else {
						Task.getJSONObject("full_progress").put("full",Resource.FLOW_INFOS.get(instance, JOB_TYPE.FULL.name()));
					}
					if (!Resource.FLOW_INFOS.containsKey(instance, JOB_TYPE.INCREMENT.name())
							|| Resource.FLOW_INFOS.get(instance, JOB_TYPE.INCREMENT.name()).size() == 0) {
						Task.put("incremental_progress", "none");
					} else {
						Task.put("incremental_progress",
								Resource.FLOW_INFOS.get(instance, JOB_TYPE.INCREMENT.name()));
					}
					Task.put("full_thread_status", EFMonitorUtil.threadStateInfo(instance, GlobalParam.JOB_TYPE.FULL));
					Task.put("incremental_thread_status", EFMonitorUtil.threadStateInfo(instance, GlobalParam.JOB_TYPE.INCREMENT));					
				}	
			}
			JO.put("reader", Reader);
			JO.put("computer", Computer);
			JO.put("writer", Writer); 
			JO.put("searcher", Searcher);
			JO.put("task", Task);
			JO.put("node_info", nodeInfo);
		} 		
		return JO;
	}
	
	public static void restartSystem() {
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				EFNodeUtil.runShell(GlobalParam.StartConfig.getProperty("restart_shell"));
			}
		});
		thread.start();
	}
}