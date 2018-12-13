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
	private double learn_rate = 0.1;
	private double threshold = 0.001;

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
	
	public static void setKeyValue(ComputeParam cp,String k, String v) {
		switch (k.toLowerCase()) {
			case "features":
				cp.features = v;
				break;
			case "value":
				cp.value = v;
				break;
			case "algorithm":
				cp.algorithm = v;
				break; 
			case "learn_rate":
				cp.learn_rate = Double.parseDouble(v);
				break; 
			case "threshold":
				cp.threshold = Double.parseDouble(v);
				break; 
		}
	}
}
