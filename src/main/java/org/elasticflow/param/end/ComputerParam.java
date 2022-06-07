/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.param.end;

import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticflow.config.GlobalParam.COMPUTER_MODE;

import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class ComputerParam {
	
	private COMPUTER_MODE computeMode = COMPUTER_MODE.BLANK;
	
	private volatile CopyOnWriteArrayList<String> api = new CopyOnWriteArrayList<>();
	/**reader and request fields map*/
	private JSONObject apiRequest = new JSONObject();
	/** api max send data nums per request**/
	private int apiRequestMaxDatas = 30;
	/**writer and response fields map*/
	private JSONObject apiResponse = new JSONObject();
	
	private String keyField;
	/** value= int or string */
	private String keyFieldType;
	private String scanField;
	private String pyPath;
	 
	private String handler;
	
	/**User defined JSON parameters can be used to extend the plugin*/
	private JSONObject customParams = new JSONObject();
	 
	public COMPUTER_MODE getComputeMode() {
		return computeMode;
	}

	public void setComputeMode(String computeMode) {
		computeMode = computeMode.strip().toLowerCase();
		if(COMPUTER_MODE.MODEL.name().toLowerCase().equals(computeMode)) { 
			this.computeMode = COMPUTER_MODE.MODEL;
		}else if(COMPUTER_MODE.REST.name().toLowerCase().equals(computeMode)) {
			this.computeMode = COMPUTER_MODE.REST;
		}else { 
			this.computeMode = COMPUTER_MODE.BLANK;
		}
	}
	
	public String getAlgorithm() {
		switch(this.computeMode) {
		case REST:
			return "org.elasticflow.ml.RestService";
		case MODEL:
			return "org.elasticflow.ml.ModelService";
		default:
			return "org.elasticflow.ml.BlankService";
		}
	}

	public String getPyPath() {
		return pyPath;
	}

	public void setPyPath(String pyPath) {
		this.pyPath = pyPath;
	}

	public String getKeyField() {
		return keyField;
	}

	public void setKeyField(String keyField) {
		this.keyField = keyField;
	}

	public String getScanField() {
		return scanField;
	}

	public void setScanField(String scanField) {
		this.scanField = scanField;
	} 
	
	public String getKeyFieldType() {
		return keyFieldType;
	}

	public void setKeyFieldType(String keyFieldType) {
		this.keyFieldType = keyFieldType;
	} 

	public CopyOnWriteArrayList<String> getApi() {
		return api;
	}

	public void setApi(String api) {
		if(!this.api.isEmpty()) 
			this.api.clear();
		if(api!=null && api.strip().length()>0) {
			for(String url:api.strip().split(","))
				this.api.add(url);
		}		
	}
	
	public int apiRequestMaxDatas() {
		return this.apiRequestMaxDatas;
	}

	public JSONObject getApiRequest() {
		return this.apiRequest;
	}

	public void setApiRequest(String apiRequest) {
		if(apiRequest!=null) {
			this.apiRequest = JSONObject.parseObject(apiRequest);
		}		
	}
	
	public void setApiRequestMaxDatas(String apiRequestMaxDatas) {
		if(apiRequestMaxDatas!=null)
			this.apiRequestMaxDatas = Integer.parseInt(apiRequestMaxDatas);
	}

	public JSONObject getApiResponse() {
		return apiResponse;
	}

	public void setApiResponse(String apiResponse) {
		if(apiResponse!=null) {
			this.apiResponse = JSONObject.parseObject(apiResponse);
		}
	}

	public JSONObject getCustomParams() {
		return customParams;
	}

	public void setCustomParams(String customParams) {
		if(customParams!=null) {
			this.customParams = JSONObject.parseObject(customParams);
		}	
	}
	
	public String getHandler() {
		return handler;
	} 
	
	public void setHandler(String handler) {
		this.handler = handler;
	} 

}
