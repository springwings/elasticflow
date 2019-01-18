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
	private double learn_rate = 0.1;
	private double threshold = 0.001;
	/**flow|batch,Flow Computing and Batch Computing*/
	private String computeModel="batch";

	public String getFeatures() {
		return features;
	} 

	public String getValue() {
		return value;
	} 
	
	public String getAlgorithm() {
		return algorithm;
	}  
	
	public double getLearn_rate() {
		return learn_rate;
	} 

	public double getThreshold() {
		return threshold;
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

	public String getPreprocessing() {
		return preprocessing;
	}

	public void setPreprocessing(String preprocessing) {
		this.preprocessing = preprocessing;
	}

	public String getComputeModel() {
		return computeModel;
	}

	public void setComputeModel(String computeModel) {
		this.computeModel = computeModel.toLowerCase();
	} 
 
}
