package org.elasticflow.model;

import java.util.HashMap;

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
	
	private int keepPeriod = 7;

	/** Batch processing blocking statistics **/
	protected long BLOCKTIME = 0;

	/** Average load of the flow,the amount of data processed per second **/
	protected long LOAD = -1;

	/** Transient performance,the amount of data processed per second in batch **/
	protected long PERFORMANCE = -1;

	/** Total amount of historical real-time data processed **/
	private long totalProcess = 0;

	/** Real time statistics of current time period **/
	private long currentTimeProcess = 0;

	/** Amount of data processed history **/
	private JSONObject historyProcess;
	
	private long flowStartTime = Common.getNow();
	
	private HashMap<String, Object> flowEndStatus;
	
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
				this.totalProcess = _JO.getLong("totalProcess");
				this.flowStartTime = _JO.getLong("flowStartTime");
				this.historyProcess = _JO.getJSONObject("historyProcess");	
			}							
		}
		this.flowEndStatus = toHashObject();
		if(this.historyProcess == null)
			this.historyProcess = new JSONObject();
		if(this.historyProcess.containsKey(todayZero)) {
			this.currentTimeProcess = this.historyProcess.getLongValue(todayZero);
		}
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
		JO.put("historyProcess", this.historyProcess==null?"":this.historyProcess);
		JO.put("performance", this.PERFORMANCE);
		JO.put("avgload", this.LOAD);
		JO.put("blocktime", this.BLOCKTIME);
	}
	
	public long getCurrentTimeProcess() {
		return this.currentTimeProcess;
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

	public long getBlockTime() {
		return this.BLOCKTIME;
	}

	public void setLoad(long load) {
		this.LOAD = load;
	}

	public void setPerformance(long performance) {
		if (performance > this.PERFORMANCE)
			this.PERFORMANCE = performance;
	}

	public void resetBlockTime() {
		this.BLOCKTIME = 0L;
	}

	public void incrementBlockTime() {
		this.BLOCKTIME += 1;
	}

	public long getTotalProcess() {
		return totalProcess;
	} 
	
	public void incrementCurrentTimeProcess(int delta) {
		todayZero = String.valueOf(Common.getNowZero());
		if(!this.historyProcess.containsKey(todayZero)) { 
			if(this.historyProcess.size()>keepPeriod) {
				String minkey = (String) Common.getMinKey(this.historyProcess.keySet());
				this.historyProcess.remove(minkey);
			}
			this.currentTimeProcess = delta;
		}else {
			this.currentTimeProcess += delta;
		}
		this.totalProcess += delta;
		this.historyProcess.put(todayZero, this.currentTimeProcess);
		this.updateDatas(this.flowEndStatus);
	}
}
