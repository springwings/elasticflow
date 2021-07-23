/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.param.ml;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class ComputeParam {
	
	private String features;
	private String value;
	private String algorithm;
	private String preprocessing;
	private String postprocessing;
	private double learn_rate = 0.1;
	private double threshold = 0.001;
	/** flow|batch,Streaming calculation or batch calculation */
	private String computeType = "batch";
	/** train,test,predict **/
	private String stage = "train";

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
		this.stage = stage;
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

}
