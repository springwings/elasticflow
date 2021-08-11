package org.elasticflow.ml.algorithm;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.elasticflow.field.EFField;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.computer.SamplePoint;
import org.elasticflow.model.computer.SampleSets;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.EFException;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-13 09:21
 */
public class LinearRegression extends Regression {
	 
	public static LinearRegression getInstance(final ConnectParams connectParams) {
		LinearRegression o = new LinearRegression();
		o.INIT(connectParams);
		return o;
	}
  
	/**
	 * batch train
	 * @param context
	 * @param samples
	 * @param transParam
	 * @return
	 * @throws EFException 
	 */
	@Override
	public DataPage train(Context context, DataSetReader DSR, Map<String, EFField> transParam) throws EFException {
		LinearRegression LR = new LinearRegression();
		SampleSets samples = SampleSets.getInstance(DSR.getDataNums()); 
		while (DSR.nextLine()) {
			samples.addPoint(DSR.getLineData(), context.getInstanceConfig().getComputeParams());
		}
		double[] para = new double[samples.getData()[0].feathures_num];
		double rate = context.getInstanceConfig().getComputeParams().getLearn_rate();
		double th = context.getInstanceConfig().getComputeParams().getThreshold();
		LR.Initialize(samples);
		LR.setPara(para, rate, th);
		LR.Update();
		DataPage DP = new DataPage();
		PipeDataUnit du = new PipeDataUnit();
		ConcurrentLinkedQueue<PipeDataUnit> dataUnit = new ConcurrentLinkedQueue<>();
		du.addFieldValue("model", LR.getModel(), transParam);
		StringBuffer sf = new StringBuffer();
		sf.append(context.getInstanceConfig().getComputeParams().getAlgorithm() + ",<br> ");
		sf.append(context.getInstanceConfig().getComputeParams().getValue() + ",<br> ");
		sf.append(context.getInstanceConfig().getComputeParams().getFeatures());
		du.addFieldValue("remark", sf.toString(), transParam);
		dataUnit.add(du);
		DP.putData(dataUnit);
		return DP;
	}

	@Override
	public Object predict(SamplePoint point) { 
		featureSize = point.features.length;
		return PreVal(point);
	}

	public double PreVal(SamplePoint s) {
		double val = 0;
		for (int i = 0; i < featureSize; i++) {
			val += theta[i] * s.features[i];
		}
		return val;
	}

	@Override
	public void Update() {
		double former = 0;
		double latter = CostFun();
		double d = 0;
		int trainTime = 1000;
		double[] p = new double[featureSize];
		do {
			former = latter;
			for (int i = 0; i < featureSize; i++) {
				for (int j = 0; j < samNum; j++) {
					d += (PreVal(samples[j]) - samples[j].value) * samples[j].features[i];
				}
				p[i] -= (learning_rate * d) / samNum;
			}
			latter = CostFun();
			if (former - latter < 0) {
				log.warn("α is larger!!!");
				break;
			}
			trainTime--;
		} while (trainTime == 0 || former - latter > learning_rate);
		theta = p;
	}

	@Override
	public double CostFun() {
		double sum = 0;
		for (int i = 0; i < samNum; i++) {
			double p = PreVal(samples[i]);
			double d = p - samples[i].value;
			sum += d;
		}
		return sum;
	}

	@Override
	public DataPage predict(Context context,DataSetReader point) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void INIT(ConnectParams connectParams) {
		// TODO Auto-generated method stub
		
	}

}
