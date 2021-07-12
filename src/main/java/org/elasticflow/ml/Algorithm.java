package org.elasticflow.ml;

import org.elasticflow.model.computer.SamplePoint;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-22 09:04
 */
public abstract class Algorithm {
 
	
	abstract public boolean loadModel(Object datas);
	 
    /**
     * predicte the value of sample s
     * @param s : prediction sample
     * @return : predicted value
     */
	abstract public Object predict(SamplePoint point);
}
