package org.elasticflow.ml.algorithm;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.elasticflow.ml.Algorithm;
import org.elasticflow.model.computer.SamplePoint;
import org.elasticflow.model.computer.SampleSets;

public abstract class Regression extends Algorithm{
	
    double[] theta;  
    int featureSize; 
    double learning_rate;  
    SamplePoint[] samples;  
    int samNum;  
    double threshold; 
    protected final static Logger log = LoggerFactory.getLogger("Regression");
    
    /**
     * initialize the samples
     * @param s : training set
     * @param num : the number of training samples
     */
    public void Initialize(SampleSets samples) {
        samNum = samples.samplesNums();
        this.samples = samples.getData(); 
    }
    
    @Override
    public boolean loadModel(Object datas) {
    	ArrayList<Double> rs = new ArrayList<>();
    	for(String s:String.valueOf(datas).split(",")) {
    		if(s.length()>0)
    			rs.add(Double.parseDouble(s));
    	}
    	theta = new double[rs.size()];
    	int i=0;
    	for(Double d:rs) {
    		theta[i] = d;
    		i++;
    	}
    	return true;
    } 
    
    /**
     * initialize all parameters
     * @param para : theta
     * @param learning_rate 
     * @param threshold 
     */
    public void setPara(double[] theta, double learning_rate, double threshold) {
    	featureSize = theta.length;
        this.theta = theta;
        this.learning_rate = learning_rate;
        this.threshold = threshold;
    }
    
    /**
     * calculate the cost of all samples
     * @return : the cost
     */
    public abstract double CostFun();
    
    /**
     * update the theta
     */
    public abstract void Update();
    
    public String getModel() {
    	StringBuffer sf = new StringBuffer();
    	for(int i = 0; i < featureSize; i++) {
    		sf.append(theta[i] + ","); 
        }
    	return sf.toString();
    }
    public void OutputTheta() {
        System.out.println("The parameters are:");
        for(int i = 0; i < featureSize; i++) {
            System.out.print(theta[i] + " ");
        }
        System.out.println(CostFun());
    }
}