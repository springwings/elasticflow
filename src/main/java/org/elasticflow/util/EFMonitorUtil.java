package org.elasticflow.util;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.config.GlobalParam.INSTANCE_STATUS;
import org.elasticflow.config.GlobalParam.INSTANCE_TYPE;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.GlobalParam.TASK_FLOW_SINGAL;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.model.EFRequest;
import org.elasticflow.model.task.FlowStatistic;
import org.elasticflow.node.NodeMonitor;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.piper.PipePump;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.yarn.Resource;

import com.alibaba.fastjson.JSONObject;

/**
 * @author chengwen
 * @version 3.0
 * @date 2018-10-25 09:08
 */

public class EFMonitorUtil {

	public static boolean checkParams(NodeMonitor obj, EFRequest rq, String checkParams) {
		for (String param : checkParams.split(",")) {
			if (rq.getParam(param) == null || rq.getStringParam(param).strip().equals("")) {
				obj.setResponse(RESPONSE_STATUS.ParameterErr, checkParams + " parameters may be missing!", null);
				return false;
			}
		}
		return true;
	}

	/**
	 * reload instance
	 * 
	 * @param instance
	 * @param reset    will reload resource.xml and so on
	 * @param runType
	 * @throws EFException
	 */
	public static void reloadInstance(String instance, String reset, String runType) throws EFException {
		EFMonitorUtil.controlInstanceState(instance, TASK_FLOW_SINGAL.Stop, true);
		int type = Resource.nodeConfig.getInstanceConfigs().get(instance).getInstanceType();
		if (runType != null) {
			type = Integer.parseInt(runType);
		}
		String instanceConfig = instance;
		if (type > 0) {
			instanceConfig = instance + ":" + type;
		}
		Resource.flowProgress.remove(instance, JOB_TYPE.FULL.name());
		Resource.flowProgress.remove(instance, JOB_TYPE.INCREMENT.name());

		// control slave node
		if (GlobalParam.DISTRIBUTE_RUN) {
			GlobalParam.INSTANCE_COORDER.distributeCoorder().removeInstanceFromCluster(instanceConfig, true);
		}
		String alias = Resource.nodeConfig.getInstanceConfigs().get(instance).getAlias();
		Resource.nodeConfig.getSearchConfigs().remove(alias);
		EFPipeUtil.removeInstance(instance, true, true);
		if (reset != null && reset.equals("true")) {
			Resource.nodeConfig.loadConfig(instanceConfig, true);
		} else {
			Resource.nodeConfig.loadConfig(instanceConfig, false);
		}
		EFMonitorUtil.rebuildFlowGovern(instanceConfig, !GlobalParam.DISTRIBUTE_RUN);
		// control slave node
		if (GlobalParam.DISTRIBUTE_RUN) {
			GlobalParam.INSTANCE_COORDER.distributeCoorder().reloadClusterInstance(instanceConfig,
					reset.equals("true"));
		}
		EFMonitorUtil.controlInstanceState(instance, TASK_FLOW_SINGAL.Ready, true);
	}

	public static void saveNodeConfig() throws Exception {
		OutputStream os = null;
		os = new FileOutputStream(GlobalParam.SYS_CONFIG_PATH.replace("file:", "") + "/config.properties");
		GlobalParam.SystemConfig.store(os, null);
	}

	/**
	 * add instance to properties file
	 * 
	 * @param instanceSetting
	 */
	public static void addInstanceToConfig(String instanceSetting) {
		String[] instanceSets = instanceSetting.split(":");
		boolean isset = false;
		String tmp = "";
		for (String str : GlobalParam.SystemConfig.getProperty("instances").split(",")) {
			String[] s = str.split(":");
			if (s[0].equals(instanceSets[0])) {
				tmp += "," + instanceSetting;
				isset = true;
			} else {
				tmp += "," + str;
			}
		}
		if (!isset)
			tmp += "," + instanceSetting;

		if (tmp.length() > 2)
			tmp = tmp.substring(1);
		GlobalParam.SystemConfig.setProperty("instances", tmp);
	}

	public static void addInstanceToSystem(String instance, String level) {
		String instanceString = instance + ":" + level;
		if (GlobalParam.DISTRIBUTE_RUN) {
			GlobalParam.INSTANCE_COORDER.loadInstance(instanceString, false, false);
			GlobalParam.INSTANCE_COORDER.distributeCoorder().pushInstanceToCluster(instanceString);
		} else {
			GlobalParam.INSTANCE_COORDER.loadInstance(instanceString, true, false);
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
		for (String str : GlobalParam.SystemConfig.getProperty("instances").split(",")) {
			String[] s = str.split(":");
			if (s[0].equals(instance))
				continue;
			tmp += str + ",";
		}
		GlobalParam.SystemConfig.setProperty("instances", tmp);
	}

	public static void rebuildFlowGovern(String instanceSettting, boolean createSchedule) throws EFException {
		for (String inst : instanceSettting.split(",")) {
			String[] strs = inst.split(":");
			if (strs.length < 1)
				continue;
			Resource.flowCenter.addFlowGovern(strs[0], Resource.nodeConfig.getInstanceConfigs().get(strs[0]), true,
					createSchedule);
		}
	}

	public static void cleanAllInstance(boolean waitComplete) {
		Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
		for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) {
			GlobalParam.INSTANCE_COORDER.removeInstance(entry.getKey(), waitComplete);
		}
	}

	public static String[] getInstanceL1seqs(String instance) {
		InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instance);
		WarehouseParam dataMap = Resource.nodeConfig.getWarehouse().get(instanceConfig.getPipeParams().getReadFrom());
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
			if (GlobalParam.TASK_COORDER.checkFlowSingal(instance, seq, type, TASK_FLOW_SINGAL.Stop))
				state = "stop";
			if (GlobalParam.TASK_COORDER.checkFlowSingal(instance, seq, type, TASK_FLOW_SINGAL.Ready))
				state = "Ready";
			if (GlobalParam.TASK_COORDER.checkFlowSingal(instance, seq, type, TASK_FLOW_SINGAL.Running))
				state = "Running";
			if (GlobalParam.TASK_COORDER.checkFlowSingal(instance, seq, type, TASK_FLOW_SINGAL.Termination))
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
	public static void controlInstanceState(String instance, TASK_FLOW_SINGAL state, boolean isIncrement) {
		InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instance);
		if ((GlobalParam.SERVICE_LEVEL & 6) == 0) {
			return;
		}
		JOB_TYPE controlType;
		if (isIncrement) {
			if (instanceConfig.getPipeParams().getDeltaCron() == null)
				return;
			controlType = GlobalParam.JOB_TYPE.INCREMENT;
		} else {
			if (instanceConfig.getPipeParams().getFullCron() == null)
				return;
			controlType = GlobalParam.JOB_TYPE.FULL;
		}

		for (String inst : instance.split(",")) {
			int waittime = 0;
			String[] seqs = EFMonitorUtil.getInstanceL1seqs(instance);
			Common.LOG.info("Instance {} seq nums {},job type {},waitting set state {} ...", inst, seqs.length,
					controlType.name(), state);
			for (String L1seq : seqs) {
				if (GlobalParam.TASK_COORDER.checkFlowSingal(inst, L1seq, controlType, TASK_FLOW_SINGAL.Running)) {
					GlobalParam.TASK_COORDER.setFlowSingal(inst, L1seq, controlType.name(), TASK_FLOW_SINGAL.Blank,
							TASK_FLOW_SINGAL.Termination, true);
					while (!GlobalParam.TASK_COORDER.checkFlowSingal(inst, L1seq, controlType, TASK_FLOW_SINGAL.Ready)) {
						try {
							waittime++;
							Thread.sleep(100);
							if (waittime > 200) {
								break;
							}
						} catch (InterruptedException e) {
							Common.LOG.error("control instance state thread sleep is interrupted exception", e);
						}
					}
					if (GlobalParam.TASK_COORDER.setFlowSingal(inst, L1seq, controlType.name(), TASK_FLOW_SINGAL.Termination,
							state, true)) {
						Common.LOG.info("Instance {} L1seq {},job type {} success set state {}.", inst,
								(L1seq.length() == 0 ? GlobalParam.DEFAULT_SEQ : L1seq), controlType.name(), state);
					} else {
						Common.LOG.info("Instance {} L1seq {},job type {} fail set state {}.", inst,
								(L1seq.length() == 0 ? GlobalParam.DEFAULT_SEQ : L1seq), controlType.name(), state);
					}
				} else {
					if (GlobalParam.TASK_COORDER.setFlowSingal(inst, L1seq, controlType.name(), TASK_FLOW_SINGAL.Blank,
							state, true)) {
						Common.LOG.info("Instance {} L1seq {},job type {} success set state {}.", inst,
								(L1seq.length() == 0 ? GlobalParam.DEFAULT_SEQ : L1seq), controlType.name(), state);
					} else {
						Common.LOG.info("Instance {} L1seq {},job type {} fail set state {}.", inst,
								(L1seq.length() == 0 ? GlobalParam.DEFAULT_SEQ : L1seq), controlType.name(), state);
					}
				}
			}
		}
	}

	public static String getConnectionStatus(String instance, String poolName) {
		if (GlobalParam.DISTRIBUTE_RUN) {
			return GlobalParam.INSTANCE_COORDER.distributeCoorder().getConnectionStatus(instance, poolName);
		} else {
			return EFConnectionPool.getStatus(poolName);
		}
	}

	public static void resetPipeEndStatus(String instance, String L1seq) {
		if (Resource.socketCenter.containsKey(instance, L1seq, GlobalParam.FLOW_TAG._DEFAULT.name())) {
			try {
				PipePump pipePump = Resource.socketCenter.getPipePump(instance, L1seq, false,
						GlobalParam.FLOW_TAG._DEFAULT.name());
				InstanceConfig config = Resource.nodeConfig.getInstanceConfigs().get(instance);
				if ((config.getInstanceType() & INSTANCE_TYPE.Trans.getVal()) > 0) {
					pipePump.getReader().flowStatistic.reset();
					pipePump.getWriter().flowStatistic.reset();
				}
				if ((config.getInstanceType() & INSTANCE_TYPE.WithCompute.getVal()) > 0)
					pipePump.getComputer().flowStatistic.reset();
			} catch (EFException e) {
				Common.LOG.error("reset instance {} L1seq {} PipeEnd status exception", instance, L1seq, e);
			}
		}
	}

	/**
	 * get Pipe End Status
	 * 
	 * @param instance
	 * @param L1seq
	 * @return
	 */
	public static JSONObject getPipeEndStatus(String instance, String L1seq) {
		JSONObject res = new JSONObject();
		res.put(END_TYPE.reader.name(), new FlowStatistic());
		res.put(END_TYPE.computer.name(), new FlowStatistic());
		res.put(END_TYPE.writer.name(), new FlowStatistic());
		res.put("status", "offline");

		if (Resource.socketCenter.containsKey(instance, L1seq, GlobalParam.FLOW_TAG._DEFAULT.name())) {
			try {
				PipePump pipePump = Resource.socketCenter.getPipePump(instance, L1seq, false,
						GlobalParam.FLOW_TAG._DEFAULT.name());
				InstanceConfig config = Resource.nodeConfig.getInstanceConfigs().get(instance);
				if ((config.getInstanceType() & INSTANCE_TYPE.Trans.getVal()) > 0) {
					res.put(END_TYPE.reader.name(), pipePump.getReader().flowStatistic.get());
					res.put(END_TYPE.writer.name(), pipePump.getWriter().flowStatistic.get());
				}
				if ((config.getInstanceType() & INSTANCE_TYPE.WithCompute.getVal()) > 0) {
					res.put(END_TYPE.computer.name(), pipePump.getComputer().flowStatistic.get());
				}
				res.put("status", "online");
				res.put("nodeIP", GlobalParam.IP);
				res.put("nodeID", GlobalParam.NODEID);
			} catch (EFException e) {
				Common.LOG.error("get instance {} L1seq {} PipeEnd Status exception", instance, L1seq, e);
			}
		}
		return res;
	}

	public static boolean resetBreaker(String instance) throws EFException {
		if (Resource.nodeConfig.getInstanceConfigs().containsKey(instance)) {
			InstanceConfig config = Resource.nodeConfig.getInstanceConfigs().get(instance);
			String[] L1seqs = TaskUtil.getL1seqs(config);
			for (String L1seq : L1seqs) {
				if (GlobalParam.DISTRIBUTE_RUN) {
					GlobalParam.INSTANCE_COORDER.distributeCoorder().resetBreaker(instance, L1seq);
				} else {
					Resource.tasks.get(TaskUtil.getInstanceProcessId(instance, L1seq)).breaker.reset();
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * get instance detail informations.
	 * 
	 * @param instance
	 * @param type     1 pool status ++ 2 Performance status ++ 4 task status ++ 8
	 *                 task total status ++
	 * @return
	 * @throws EFException
	 */
	public static JSONObject getInstanceInfo(String instance, int type) throws EFException {
		JSONObject JO = new JSONObject();
		if (GlobalParam.DISTRIBUTE_RUN && GlobalParam.INSTANCE_COORDER.distributeCoorder().getClusterStatus() != 0)
			return JO;
		if (Resource.nodeConfig.getInstanceConfigs().containsKey(instance)) {
			InstanceConfig config = Resource.nodeConfig.getInstanceConfigs().get(instance);
			JSONObject Reader = new JSONObject();
			JSONObject Writer = new JSONObject();
			JSONObject nodeInfo = new JSONObject();
			JSONObject Computer = new JSONObject();
			JSONObject Searcher = new JSONObject();
			JSONObject Task = new JSONObject();
			if ((type & 1) > 0) {
				if (Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom()) != null) {
					WarehouseParam wp = Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getReadFrom());
					JSONObject poolstatus = new JSONObject();
					for (String seq : wp.getL1seq()) {
						if (seq == "") {
							poolstatus.put("DEFAULT", getConnectionStatus(instance, wp.getPoolName(seq)));
						} else {
							poolstatus.put(seq, getConnectionStatus(instance, wp.getPoolName(seq)));
						}

					}
					Reader.put("pool_status", poolstatus);
				}

				if (Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getWriteTo()) != null) {
					WarehouseParam wp = Resource.nodeConfig.getWarehouse().get(config.getPipeParams().getWriteTo());
					JSONObject poolstatus = new JSONObject();
					for (String seq : wp.getL1seq()) {
						if (seq == "") {
							poolstatus.put("DEFAULT", getConnectionStatus(instance, wp.getPoolName(seq)));
						} else {
							poolstatus.put(seq, getConnectionStatus(instance, wp.getPoolName(seq)));
						}
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
					String poolname = Resource.nodeConfig.getWarehouse().get(searchFrom)
							.getPoolName(GlobalParam.DEFAULT_RESOURCE_SEQ);
					Searcher.put(searcherInfo, getConnectionStatus(instance, poolname));
				}
			}

			if ((type & 2) > 0) {
				String[] L1seqs = TaskUtil.getL1seqs(config);
				for (String L1seq : L1seqs) {
					String appendPipe = "";
					if (L1seq != "")
						appendPipe = "L1seq(" + L1seq + ")_";
					JSONObject tmp;
					if (GlobalParam.DISTRIBUTE_RUN) {
						tmp = GlobalParam.INSTANCE_COORDER.distributeCoorder().getPipeEndStatus(config.getInstanceID(),
								L1seq);
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
					nodeInfo.put(appendPipe + "node_ip", tmp.get("nodeIP"));
					nodeInfo.put(appendPipe + "node_id", tmp.get("nodeID"));
					nodeInfo.put(appendPipe + "pipe_size",
							Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getReadPageSize());
					nodeInfo.put(appendPipe + "status", tmp.get("status"));
				}
			}

			if ((type & 4) > 0) {
				if (config.openTrans()) {
					String[] L1seqs = TaskUtil.getL1seqs(config);
					for (String L1seq : L1seqs) {
						String appendPipe = "";
						if (L1seq != "")
							appendPipe = "L1seq(" + L1seq + ")_";
						Task.putAll(GlobalParam.INSTANCE_COORDER.distributeCoorder()
								.getBreakerStatus(config.getInstanceID(), L1seq, appendPipe));
					}
					Task.put("incremental_storage_status",
							GlobalParam.TASK_COORDER.getInstanceScanDatas(instance, false));
					Task.put("full_storage_status", GlobalParam.TASK_COORDER.getInstanceScanDatas(instance, true));   
					Task.put("full_thread_status", EFMonitorUtil.threadStateInfo(instance, GlobalParam.JOB_TYPE.FULL));
					Task.put("incremental_thread_status",
							EFMonitorUtil.threadStateInfo(instance, GlobalParam.JOB_TYPE.INCREMENT));
					Task.put("full_progress", Resource.flowProgress.get(instance, JOB_TYPE.FULL.name()));
					Task.put("incremental_progress", Resource.flowProgress.get(instance, JOB_TYPE.INCREMENT.name()));
				}
			}

			if ((type & 8) > 0) {
				if (config.openTrans()) {
					String[] L1seqs = TaskUtil.getL1seqs(config);
					boolean breakerOn = false;
					int current_fail_interval = Integer.MAX_VALUE;
					for (String L1seq : L1seqs) {
						JSONObject tmp = GlobalParam.INSTANCE_COORDER.distributeCoorder()
								.getBreakerStatus(config.getInstanceID(), L1seq, "");
						if (tmp.getBoolean("breaker_is_on"))
							breakerOn = true;
						if (tmp.getInteger("current_fail_interval") < current_fail_interval)
							current_fail_interval = tmp.getInteger("current_fail_interval");
					}
					if (breakerOn) {
						JO.put("instance_status", INSTANCE_STATUS.Error.getVal());
					} else if (current_fail_interval < 180) {
						JO.put("instance_status", INSTANCE_STATUS.Warning.getVal());
					} else {
						JO.put("instance_status", INSTANCE_STATUS.Normal.getVal());
					}
				} else {
					JO.put("instance_status", INSTANCE_STATUS.Normal.getVal());
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
				Common.LOG.info("system restart...");  
				EFNodeUtil.runShell(new String[] {"bash","-c",GlobalParam.RESTART_SHELL_COMMAND});
			}
		});
		thread.start();
	}
}