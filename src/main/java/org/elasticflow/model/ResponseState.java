package org.elasticflow.model;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-05 13:53
 */
public class ResponseState {

	protected Map<String, String> params = new HashMap<>();
	protected Map<String, String> parsedParams = new HashMap<>(); 
	protected Object payload = null;
	private long startTime = 0;
	private long endTime = 0;
	private long duration = 0; 
	private String instance = ""; 
	public Map<String, Object> response = new LinkedHashMap<>();

	public static ResponseState getInstance() {
		ResponseState rs = new ResponseState();
		rs.response.put("request", rs.params);
		rs.response.put("info", "");
		rs.response.put("err_no", "100");
		rs.response.put("err_msg", "");
		rs.response.put("env", GlobalParam.run_environment);
		rs.response.put("instance",null);
		rs.response.put("duration",null);
		return rs;
	}
	
	public void setInfo(String info) { 
		response.put("info", info);
	}  
	
	public void setError_info(String err) {
		if(err.length() > 0)
			response.put("err_no", "500"); 
		response.put("err_msg", err);
	}  

	public Map<String, String> getParams() {
		return params;
	}

	public void setParams(Map<String, String> params, InstanceConfig prs) {
		if (prs != null) {
			for (Map.Entry<String, String> entry : params.entrySet()) {
				this.params.put(entry.getKey(), entry.getValue());
			}
		}
	}   

	public Object getPayload() {
		return payload;
	}

	public void setPayload(Object payload) {
		this.payload = payload;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
		this.duration = this.endTime - this.startTime;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}
 
	public void setInstance(String instance) {
		this.instance = instance;
	}

	public String getResponse(boolean isJson) {
		if(isJson) {
			return JSON.toJSONString(formatData());
		}else {
			return formatData().toString();
		}
	} 
	
	private Map<String, Object> formatData() { 
		Map<String, Object> rsp = new LinkedHashMap<String, Object>();  
		response.put("instance", this.instance);
		response.put("duration", String.valueOf(getDuration()) + "ms");
		 
		Map<String, String> paramsMap = new HashMap<String, String>();
		paramsMap.putAll(params);
		if (payload != null) {
			rsp.put("results", payload);
		}
		response.put("response", rsp);
		return response;
	}  
}
