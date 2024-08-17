package org.elasticflow.yarn.coordinator;

import java.util.HashMap;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.SystemInfoUtil;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.master.EFMonitorCoord;

import com.alibaba.fastjson.JSONObject;

/**
 * EFMonitor Coordinator
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */

public class EFMonitorCoordinator implements EFMonitorCoord {

	@Override
	public String getPoolStatus(String poolName) {
		return EFConnectionPool.getStatus(poolName);
	}

	@Override
	public JSONObject getPipeEndStatus(String instance, String L1seq) { 
		return EFMonitorUtil.getPipeEndStatus(instance, L1seq);
	}
	
	@Override
	public String analyzeInstance(String instance) { 
		return EFMonitorUtil.analyzeInstance(instance);
	}
	
	@Override
	public HashMap<String, JSONObject> getResourceStates() {
		return EFMonitorUtil.getResourceStates();
	}
	
	@Override
	public String getLogs(int lines) {
		return EFFileUtil.readLastNLines(GlobalParam.lOG_STORE_PATH, lines);
	}
	
	@Override
	public void resetPipeEndStatus(String instance, String L1seq) {
		EFMonitorUtil.resetPipeEndStatus(instance, L1seq);
	}
	
	@Override
	public void resetErrorStates() {
		Resource.resetErrorStates();
	}

	@Override
	public HashMap<String, Object> getNodeStatus() {
		HashMap<String, Object> dt = new HashMap<>();
		dt.put("WRITE_BATCH", GlobalParam.WRITE_BATCH);
		dt.put("VERSION", GlobalParam.VERSION);
		dt.put("LANG", GlobalParam.LANG);
		dt.put("NODE_ID", GlobalParam.NODEID);
		dt.put("TASKS", Resource.tasks.size());
		dt.put("THREAD_POOL_SIZE", Resource.threadPools.getPoolSize());
		dt.put("THREAD_ACTIVE_COUNT", Resource.threadPools.getActiveCount());
		dt.put("SYS_THREAD_POOL_SIZE", GlobalParam.SYS_THREADPOOL_SIZE);
		dt.put("MEMORY", SystemInfoUtil.getMemTotal());
		dt.put("MEMORY_USAGE", SystemInfoUtil.getMemUsage());
		dt.put("CPU_USAGE", SystemInfoUtil.getCpuUsage());
		dt.put("ERROR_IGNORE", Resource.getErrorStates(ELEVEL.Ignore));
		dt.put("ERROR_DISPOSE", Resource.getErrorStates(ELEVEL.Dispose));
		dt.put("ERROR_BREAKOFF", Resource.getErrorStates(ELEVEL.BreakOff));
		dt.put("ERROR_TERMINATION", Resource.getErrorStates(ELEVEL.Termination));
		return dt;
	}

}
