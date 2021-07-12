package org.elasticflow.model.computer;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-22 09:08
 */
public class SamplePoint {
	public double[] features;
	public int feathures_num;
	public double value;
	public int label;

	public SamplePoint(int feathures_num) {
		this.feathures_num = feathures_num;
		features = new double[feathures_num];
	}

}
