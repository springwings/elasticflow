package org.elasticflow.util;

import java.io.FileOutputStream;
import java.io.OutputStream;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.INSTANCE_TYPE;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.node.NodeMonitor;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.yarn.Resource;
import org.mortbay.jetty.Request;

/**
 * @author chengwen
 * @version 3.0
 * @date 2018-10-25 09:08
 */

public class EFMonitorUtil {
	
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
			Common.LOG.info("Instance " + inst + " waitting set state " + state + " ...");
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
					Common.LOG.info("Instance " + inst + " success set state " + state);
				} else {
					Common.LOG.info("Instance " + inst + " fail set state " + state);
				}
			}
		}
	}
}
