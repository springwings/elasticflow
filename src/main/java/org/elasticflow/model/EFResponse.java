/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.model;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;

import com.alibaba.fastjson.JSON;

/**
 * ElasticFlow response model
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-05 13:53
 */
public class EFResponse {
	
	protected Map<String, String> parsedParams = new HashMap<>();
	private static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected Object payload = null;
	private long startTime = 0;
	private long endTime = 0;
	private String instance = "";
	private Map<String, Object> request;
	public Map<String, Object> response = new LinkedHashMap<>();
	
	
	public static EFResponse getInstance() {
		EFResponse rs = new EFResponse();
		rs.response.put("status", RESPONSE_STATUS.Success.getVal());
		rs.response.put("info", RESPONSE_STATUS.Success);  
		return rs;
	}

	public void setStatus(String info, GlobalParam.RESPONSE_STATUS status) {
		if(info!=null) {
			response.put("info", status.getMsg()+","+info); 
		}else {
			response.put("info", status.getMsg()); 
		}		
		response.put("status", status.getVal());
	}

	public void setInfo(String info) {
		response.put("info", info);
		response.put("status", GlobalParam.RESPONSE_STATUS.Success.getVal());
	}

	public void setRequest(Map<String, Object> request) {
		this.request = request;
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
	}

	public long getDuration() {
		return this.endTime - this.startTime;
	} 

	public void setInstance(String instance) {
		this.instance = instance;
	}

	public String getResponse(boolean isJson) {
		if (isJson) {
			return JSON.toJSONString(formatData());
		} else {
			return formatData().toString();
		}
	}

	private Map<String, Object> formatData() {
		Map<String, Object> rsp = new LinkedHashMap<String, Object>();
		if(!request.containsKey(GlobalParam.CLOSE_REQUEST_RESPONSE) || 
				!request.get(GlobalParam.CLOSE_REQUEST_RESPONSE).toString().toLowerCase().equals("true")) {
			response.put("request", request);
		}
		response.put("instance", this.instance);
		response.put("duration", String.valueOf(getDuration()) + "ms");
		if (payload != null) {
			rsp.put("datas", payload);
		}
		response.put("response", rsp);
		response.put("createTime", SDF.format(System.currentTimeMillis()));
		response.put("__SOURCE", GlobalParam.PROJ);
		response.put("__VERSION", GlobalParam.VERSION);
		response.put("__ENV", GlobalParam.RUN_ENV);
		response.put("__IS_DEBUG", GlobalParam.DEBUG);
		response.put("__SYS_START_TIME", SDF.format(GlobalParam.SYS_START_TIME));
		return response;
	}
}
