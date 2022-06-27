package org.elasticflow.model;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.util.Common;

import com.alibaba.fastjson.JSONObject;

/**
 * Flow statistical information
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-08 16:49
 */
final public class FlowState {
	
	/** FAIL processing data units statistics **/
	private volatile AtomicInteger failProcess = new AtomicInteger(0);

	/** Batch processing blocking statistics **/
	private volatile AtomicInteger BLOCKTIME = new AtomicInteger(0);

	/** Average load of the flow,the amount of data processed per second **/
	private volatile long LOAD = -1;

	/** Transient performance,the amount of data processed per second in batch **/
	private volatile long PERFORMANCE = -1;

	/** Total amount of historical real-time data processed **/
	private volatile AtomicLong totalProcess = new AtomicLong(0);

	/** Real time statistics of current time period **/
	private volatile AtomicLong currentTimeProcess = new AtomicLong(0);

	/** Amount of data processed history **/
	private volatile JSONObject historyProcess;
	
	private long flowStartTime = Common.getNow();
	
	private volatile HashMap<String, Object> flowEndStatus;
	
	private String todayZero = String.valueOf(Common.getNowZero());
		
	public static String getStoreKey(String L1seq) {
		String storeKey;
		if(L1seq!=null && L1seq.length()>0) {
			storeKey = L1seq; 
		}else {
			storeKey = "__"; 
		} 
		return storeKey;
	}
	
	/**
	 * example
	 * {
	 *   "seq":{"reader":{},"computer":{}},
	 *   "__":{"reader":{},"computer":{}},
	 * }
	 * @param stat
	 * @param endType
	 * @param L1seq
	 */
	public FlowState(JSONObject stat,END_TYPE endType,String L1seq) {
		String storeKey = getStoreKey(L1seq);
		if(stat.containsKey(storeKey)) {
			JSONObject JO = stat.getJSONObject(storeKey);
			if(JO.containsKey(endType.name())) {
				JSONObject _JO = JO.getJSONObject(endType.name());
				this.totalProcess.set(_JO.getLong("totalProcess"));
				this.flowStartTime = _JO.getLong("flowStartTime");
				this.historyProcess = _JO.getJSONObject("historyProcess");	
			}							
		}
		if(this.historyProcess == null) 
			this.historyProcess = new JSONObject();
		if(this.historyProcess.containsKey(todayZero)) {
			this.currentTimeProcess.set(this.historyProcess.getLongValue(todayZero));			
		}else {
			this.historyProcess.put(String.valueOf(Common.getNowZero()), 0);
		}
		this.flowEndStatus = toHashObject();
	} 
	
	public HashMap<String, Object> get() {		
		return this.flowEndStatus;
	}
	
	private HashMap<String, Object> toHashObject() {
		HashMap<String, Object> JO = new HashMap<>();
		this.updateDatas(JO);
		return JO;
	}
	
	private void updateDatas(HashMap<String, Object> JO) {		
		JO.put("totalProcess", this.totalProcess);
		JO.put("currentTimeProcess", this.currentTimeProcess);
		JO.put("flowStartTime", this.flowStartTime);
		JO.put("historyProcess", this.historyProcess);
		JO.put("performance", this.PERFORMANCE);
		JO.put("avgload", this.LOAD);
		JO.put("blockTime", this.BLOCKTIME);
		JO.put("failProcess", this.failProcess);
	}
	
	
	public long getFlowStartTime() {
		return this.flowStartTime;
	}

	public long getLoad() {
		return this.LOAD;
	}

	public long getPerformance() {
		return this.PERFORMANCE;
	}

	public void setLoad(long load) {
		this.LOAD = load;
		this.updateDatas(this.flowEndStatus);
	}

	public void setPerformance(long performance) {
		if (performance > this.PERFORMANCE) {
			this.PERFORMANCE = performance;
			this.updateDatas(this.flowEndStatus);
		}	
	}

	public void resetBlockTime() {
		this.BLOCKTIME.set(0);
	}

	public void incrementBlockTime() {
		this.BLOCKTIME.incrementAndGet();
	}
	
	public void incrementFailUnitTime() {
		this.failProcess.incrementAndGet();
	}
	
	public void incrementFailUnitTime(int val) {
		this.failProcess.addAndGet(val);
	} 
	
	public void incrementCurrentTimeProcess(int delta) {
		todayZero = String.valueOf(Common.getNowZero());
		if(!this.historyProcess.containsKey(todayZero)) { 
			if(this.historyProcess.size()>GlobalParam.INSTANCE_STATISTICS_KEEP_PERIOD) {
				String minkey = (String) Common.getMinKey(this.historyProcess.keySet());
				this.historyProcess.remove(minkey);
			}
			this.currentTimeProcess.set(delta);
		}else {
			this.currentTimeProcess.addAndGet(delta);
		}
		this.totalProcess.addAndGet(delta);
		this.historyProcess.put(todayZero, this.currentTimeProcess.get());
		this.updateDatas(this.flowEndStatus);
	}
}
