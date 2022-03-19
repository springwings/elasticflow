package org.elasticflow.yarn.coordinator;

import java.util.HashMap;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.connection.EFConnectionPool;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFMonitorUtil;
import org.elasticflow.util.SystemInfoUtil;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coord.EFMonitorCoord;

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
	public HashMap<String, Object> getNodeStatus() {
		HashMap<String, Object> dt = new HashMap<>();
		dt.put("WRITE_BATCH", GlobalParam.WRITE_BATCH);
		dt.put("VERSION", GlobalParam.VERSION);
		dt.put("TASKS", Resource.tasks.size());
		dt.put("THREAD_POOL_SIZE", Resource.ThreadPools.getPoolSize());
		dt.put("THREAD_ACTIVE_COUNT", Resource.ThreadPools.getActiveCount());
		try {
			dt.put("CPU", SystemInfoUtil.getCpuUsage());
			dt.put("MEMORY", SystemInfoUtil.getMemUsage());
		} catch (Exception e) {
			Common.LOG.error(" getStatus Exception ", e);
		}
		return dt;
	}

}
