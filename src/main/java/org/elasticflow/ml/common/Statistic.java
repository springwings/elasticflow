package org.elasticflow.ml.common;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-12-05 10:15
 */
public final class Statistic {
	
	public static double conditionalEntropy() {
		return 0; 
	}

	public static double entropy(double[] probs) {
		double entropy = 0.0;
		for (Double prob : probs) {
			if (prob > 0) {
				entropy -= prob * Math.log(prob);
			}
		}
		return entropy;
	}

	public static double conditionalEntropy(double[] data, double[] condition) {
		return 0; 
	}
}
