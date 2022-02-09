package org.elasticflow.model;

import java.util.HashMap;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFFileUtil;

import com.alibaba.fastjson.JSONObject;

/**
 * Flow statistical information
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-08 16:49
 */
final public class FlowState {

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

	/** Amount of data processed yesterday **/
	private long yesterdayProcess = -1;
	
	private long flowStartTime = Common.getNow();
	
	private JSONObject flowStoreStatus;
	
	private HashMap<String, Object> flowEndStatus;
		
	private END_TYPE endType;
	
	private long todayZero = Common.getNowZero();

	public FlowState(String path,END_TYPE endType) {
		String content = EFFileUtil.readText(path, GlobalParam.ENCODING, true);
		this.endType = endType; 
		if (content!=null && content.length()>0) {			
			this.flowStoreStatus = JSONObject.parseObject(content);	
			if(this.flowStoreStatus.containsKey(this.endType.name())) {
				JSONObject JO = this.flowStoreStatus.getJSONObject(this.endType.name());
				this.totalProcess = JO.getLong("totalProcess");
				this.currentTimeProcess = JO.getLong("currentTimeProcess");
				this.flowStartTime = JO.getLong("flowStartTime");
				this.yesterdayProcess = JO.getLong("yesterdayProcess");	
				this.init(false);
			}else {
				this.init(true);
			}
		}else {
			this.flowStoreStatus = new JSONObject();
			this.init(true);
		} 
	} 
	
	private void init(boolean fill) {		
		this.flowEndStatus = toHashObject(); 
		if(fill) {
			this.flowStoreStatus.put(END_TYPE.reader.name(),
					this.endType==END_TYPE.reader?this.flowEndStatus:this.flowEndStatus.clone());
			this.flowStoreStatus.put(END_TYPE.writer.name(),
					this.endType==END_TYPE.writer?this.flowEndStatus:this.flowEndStatus.clone());
			this.flowStoreStatus.put(END_TYPE.computer.name(),
					this.endType==END_TYPE.computer?this.flowEndStatus:this.flowEndStatus.clone());
		}else {			
			this.flowStoreStatus.put(this.endType.name(),this.flowEndStatus);
		}		
	}
		
	public JSONObject getFlowAllStatus() {
		return this.flowStoreStatus;
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
		JO.put("yesterdayProcess", this.yesterdayProcess);
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
		if(Common.getNow()>this.todayZero+86400) { 
			this.yesterdayProcess = this.currentTimeProcess;
			this.currentTimeProcess = delta;
		}else {
			this.currentTimeProcess += delta;
		}
		this.totalProcess += delta;
		this.updateDatas(this.flowEndStatus);
	}
}
