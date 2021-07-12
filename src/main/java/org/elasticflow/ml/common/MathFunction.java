package org.elasticflow.ml.common;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-22 09:04
 */
public final class MathFunction {
	
    public static double sigmoid(double v){  
        return 1.0/(1.0+Math.exp(v));
    }
    
    public static double pointMulti(double[] v1,double[] v2) {
    	double res = 0d; 
    	for(int i=0;i<v1.length;i++) {
    		res+=v1[i]*v2[i];
    	}
    	return res;
    }
    
    public static double sigmaSqrt(double[] v) {
    	double res = 0d; 
    	for(int i=0;i<v.length;i++) {
    		res+=v[i]*v[i];
    	}
    	return Math.sqrt(res);
    }
}
