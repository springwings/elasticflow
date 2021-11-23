package org.elasticflow.util;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.INSTANCE_TYPE;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.node.NodeMonitor;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.piper.PipePump;
import org.elasticflow.yarn.Resource;
import org.mortbay.jetty.Request;

import com.alibaba.fastjson.JSONObject;

/**
 * @author chengwen
 * @version 3.0
 * @date 2018-10-25 09:08
 */

public class EFMonitorUtil {
	
	public static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static boolean checkParams(NodeMonitor obj,Request rq,String checkParams){ 
		for(String param:checkParams.split(",")) {
			if(rq.getParameter(param)==null || rq.getParameter(param).strip().equals("")) {
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
	public static void addConfigInstances(String instanceSetting) {
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

	
	public static void rebuildFlowGovern(String instanceSettting) {
		for (String inst : instanceSettting.split(",")) {
			String[] strs = inst.split(":");
			if (strs.length < 1)
				continue;
			Resource.FlOW_CENTER.addFlowGovern(strs[0], Resource.nodeConfig.getInstanceConfigs().get(strs[0]), true);
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
	
	public static String threadStateInfo(String instance, JOB_TYPE type) {
		String[] seqs = EFMonitorUtil.getInstanceL1seqs(instance);
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
	

	/**
	 * control Instance current run thread, prevent error data write
	 * 
	 * @param instance    multi-instances seperate with ","
	 * @param state
	 * @param isIncrement control thread type
	 */
	public static void controlInstanceState(String instance, STATUS state, boolean isIncrement) {
		if ((GlobalParam.SERVICE_LEVEL & 6) == 0) {
			return;
		}
		JOB_TYPE controlType = GlobalParam.JOB_TYPE.FULL;
		if (isIncrement)
			controlType = GlobalParam.JOB_TYPE.INCREMENT;

		for (String inst : instance.split(",")) {
			Common.LOG.info("Instance {} waitting set state {} ...",inst,state);
			int waittime = 0;
			String[] seqs = EFMonitorUtil.getInstanceL1seqs(instance);
			for (String seq : seqs) {
				if (Common.checkFlowStatus(inst, seq, controlType, STATUS.Running)) {
					Common.setFlowStatus(inst, seq, controlType.name(), STATUS.Blank, STATUS.Termination, true);
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
				Common.setFlowStatus(inst, seq, controlType.name(), STATUS.Blank, STATUS.Termination, true);
				if (Common.setFlowStatus(inst, seq, controlType.name(), STATUS.Termination, state, true)) {
					Common.LOG.info("Instance {} success set state {}.",inst,state);
				} else {
					Common.LOG.info("Instance {} fail set state {}.",inst,state);
				}
			}
		}
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
			JSONObject Computer = new JSONObject();
			JSONObject Searcher = new JSONObject();
			JSONObject Task = new JSONObject();
			if((type&1)>0) {
				if (Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom()) != null) {
					WarehouseParam ws = Resource.nodeConfig.getWarehouse()
							.get(config.getPipeParams().getReadFrom());
					String poolname = "";
					if (ws.getL1seq().length > 0) {
						for (String seq : ws.getL1seq()) {
							poolname = Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom())
									.getPoolName(seq);
							Reader.put("Seq(" + seq + ") Pool Status", EFConnectionPool.getStatus(poolname));
						}
					} else {
						poolname = Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom())
								.getPoolName(null);
						Reader.put("Pool Status", EFConnectionPool.getStatus(poolname));
					}
				}
				

				if (Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getWriteTo()) != null) {
					String poolname = Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getWriteTo())
							.getPoolName(null);
					Writer.put("Pool Status", EFConnectionPool.getStatus(poolname));
				}

				if ((GlobalParam.SERVICE_LEVEL & 1) > 0) {
					String searchFrom = config.getPipeParams().getSearchFrom();
					String searcherInfo = "";
					if (config.getPipeParams().getWriteTo() != null
							&& config.getPipeParams().getWriteTo().equals(searchFrom)) {
						searcherInfo = "Pool (Default Writer) Status";
					} else {
						searcherInfo = "Pool Status";
					}					
					String poolname = Resource.nodeConfig.getWarehouse().get(searchFrom).getPoolName(null);
					Searcher.put(searcherInfo, EFConnectionPool.getStatus(poolname));
					
				}
			}
			
			if((type&2)>0) {
				String[] L1seqs = Common.getL1seqs(config, true);
				for (String seq : L1seqs) {
					PipePump pipePump = Resource.SOCKET_CENTER.getPipePump(config.getName(), seq, false,
							GlobalParam.FLOW_TAG._DEFAULT.name());
					String appendPipe = "";
					if (seq != "") {
						appendPipe = "Seq(" + seq + ") ";
					}
					Reader.put(appendPipe + "Load", pipePump.getReader().getLoad());
					Reader.put(appendPipe + "Performance", pipePump.getReader().getPerformance());
					if ((config.getInstanceType() & INSTANCE_TYPE.WithCompute.getVal()) > 0) {
						Computer.put(appendPipe + "Load", pipePump.getComputer().getLoad());
						Computer.put(appendPipe + "Performance", pipePump.getComputer().getPerformance());
						Computer.put(appendPipe + "Blocked Time", pipePump.getComputer().getBlockTime());
					}
					if ((config.getInstanceType() & INSTANCE_TYPE.Trans.getVal()) > 0) {
						Writer.put(appendPipe + "Load", pipePump.getWriter().getLoad());
						Writer.put(appendPipe + "Performance", pipePump.getWriter().getPerformance());
					}
				}
			}			
				
			if((type&4)>0) {
				if (config.openTrans()) {
					WarehouseParam wsp = Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom());
					if (wsp.getL1seq().length > 0) {
						StringBuilder sb = new StringBuilder();
						StringBuilder fullstate = new StringBuilder();
						for (String seq : wsp.getL1seq()) {
							String strs = GlobalParam.SCAN_POSITION.get(instance)
									.getPositionString();
							if (strs == null)
								continue;
							sb.append("\r\n;(" + seq + ") "
									+ GlobalParam.SCAN_POSITION.get(instance).getStoreId()
									+ ":");

							for (String str : strs.split(",")) {
								String update;
								String[] dstr = str.split(":");
								if (dstr[1].length() > 9 && dstr[1].matches("[0-9]+")) {
									update = dstr[0] + ":"
											+ (SDF.format(dstr[1].length() < 12 ? Long.valueOf(dstr[1] + "000")
													: Long.valueOf(dstr[1])))
											+ " (" + dstr[1] + ")";
								} else {
									update = str;
								}
								sb.append(", ");
								sb.append(update);
							}
							fullstate.append(seq + ":" + Common.getFullStartInfo(instance, seq) + "; ");
						}
						Task.put("Incremental storage status", sb);
						Task.put("Full storage status", fullstate);
					} else {
						String strs = GlobalParam.SCAN_POSITION.get(instance)
								.getPositionString();
						if (strs.length() > 0) {
							StringBuilder stateStr = new StringBuilder();
							if (strs.split(",").length > 0) {
								for (String tm : strs.split(",")) {
									String[] dstr = tm.split(":");
									if (dstr[1].length() > 9 && dstr[1].matches("[0-9]+")) {
										stateStr.append(dstr[0] + ":"
												+ SDF.format(tm.length() < 12 ? Long.valueOf(dstr[1] + "000")
														: Long.valueOf(dstr[1])));
										stateStr.append(" (").append(tm).append(")");
									} else {
										stateStr.append(tm);
									}
									stateStr.append(", ");
								}
							}
							Task.put("Incremental storage status",
									GlobalParam.SCAN_POSITION.get(instance).getStoreId()
											+ ":" + stateStr.toString());
						}
						Task.put("Full storage status", Common.getFullStartInfo(instance, null));
					}
					if (!Resource.FLOW_INFOS.containsKey(instance, JOB_TYPE.FULL.name())
							|| Resource.FLOW_INFOS.get(instance, JOB_TYPE.FULL.name()).size() == 0) {
						Task.put("Full progress", new JSONObject().put("full","none"));
					} else {
						Task.put("Full progress",  new JSONObject().put("full",Resource.FLOW_INFOS.get(instance, JOB_TYPE.FULL.name())));
					}
					if (!Resource.FLOW_INFOS.containsKey(instance, JOB_TYPE.INCREMENT.name())
							|| Resource.FLOW_INFOS.get(instance, JOB_TYPE.INCREMENT.name()).size() == 0) {
						Task.put("Incremental progress", "increment:none");
					} else {
						Task.put("Incremental progress",
								"increment:" + Resource.FLOW_INFOS.get(instance, JOB_TYPE.INCREMENT.name()));
					}
					Task.put("Incremental thread status", EFMonitorUtil.threadStateInfo(instance, GlobalParam.JOB_TYPE.INCREMENT));
					Task.put("Full thread status", EFMonitorUtil.threadStateInfo(instance, GlobalParam.JOB_TYPE.FULL));
				}	
			}
			JO.put("Reader", Reader);
			JO.put("Computer", Computer);
			JO.put("Writer", Writer);
			JO.put("Searcher", Searcher);
			JO.put("Task", Task);
		} 		
		return JO;
	}
}
