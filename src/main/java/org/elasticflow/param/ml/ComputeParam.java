/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.param.ml;

import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class ComputeParam {
	
	/**The characteristic fields are separated by ","**/
	private String features;
	private String value;
	private String algorithm;
	private String[] api;
	/**reader and request fields map*/
	private JSONObject apiRequest;
	/**writer and response fields map*/
	private JSONObject apiResponse;
	/**User defined JSON parameters can be used to extend the plugin*/
	private JSONObject customParams;
	protected String keyField;
	/** value= int or string */
	protected String keyFieldType;
	protected String scanField;
	
	private String preprocessing;
	private String postprocessing;
	private double learn_rate = 0.1;
	private double threshold = 0.001;
	/** flow|batch,Streaming calculation or batch calculation */
	private String computeType = "batch";
	/** train,test,predict **/
	private String stage = "train";
	private String handler;
	
	
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
	
	public String getFeatures() {
		return features;
	}

	public String getValue() {
		return value;
	}

	public String getAlgorithm() {
		return algorithm;
	}

	public String getStage() {
		return stage;
	}

	public double getLearn_rate() {
		return learn_rate;
	}

	public double getThreshold() {
		return threshold;
	}

	public String getPreprocessing() {
		return preprocessing;
	}

	public String getPostprocessing() {
		return postprocessing;
	}

	public String getComputeType() {
		return computeType;
	}

	public void setFeatures(String features) {
		this.features = features;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}

	public void setLearn_rate(String learn_rate) {
		this.learn_rate = Double.parseDouble(learn_rate);
	}

	public void setThreshold(String threshold) {
		this.threshold = Double.parseDouble(threshold);
	}

	public void setStage(String stage) {
		this.stage = stage.trim().toUpperCase();
	}

	public void setPreprocessing(String preprocessing) {
		this.preprocessing = preprocessing;
	}

	public void setPostprocessing(String postprocessing) {
		this.postprocessing = postprocessing;
	}

	public void setComputeType(String computeType) {
		this.computeType = computeType.toLowerCase();
	}

	public String[] getApi() {
		return api;
	}

	public void setApi(String api) {
		if(api!=null) {
			this.api = api.split(",");
		}		
	}

	public JSONObject getApiRequest() {
		return apiRequest;
	}

	public void setApiRequest(String apiRequest) {
		if(apiRequest!=null) {
			this.apiRequest = JSONObject.parseObject(apiRequest);
		}		
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
