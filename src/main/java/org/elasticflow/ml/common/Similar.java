package org.elasticflow.ml.common;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-12-05 16:42
 */
public class Similar {

	public double cos(double[] v1, double[] v2) {
		return MathFunction.pointMulti(v1, v2) / (MathFunction.sigmaSqrt(v1) * MathFunction.sigmaSqrt(v2)); 
	}

}
