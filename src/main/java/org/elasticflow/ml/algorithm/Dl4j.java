package org.elasticflow.ml.algorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.datavec.api.records.reader.impl.collection.ListStringRecordReader;
import org.datavec.api.split.ListStringSplit;
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.elasticflow.field.RiverField;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.computer.SamplePoint;
import org.elasticflow.model.computer.SampleSets;
import org.elasticflow.model.reader.DataPage;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction;

public class Dl4j {
	
	static int seed = 123;
	static int numInputs = 6;
	static int numOutputs = 6;
	static int numHiddenNodes = 20;
	static int nEpochs = 30;
	
	public static DataPage train(Context context, SampleSets samples, Map<String, RiverField> transParam) {
		double learningRate = context.getInstanceConfig().getComputeParams().getLearn_rate();
		double th = context.getInstanceConfig().getComputeParams().getThreshold();
		DataPage DP = new DataPage();
		MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(seed)
                .updater(new Nesterovs(learningRate, 0.9))
                .list()
                .layer(0, new DenseLayer.Builder().nIn(numInputs).nOut(numHiddenNodes)
                        .weightInit(WeightInit.XAVIER)
                        .activation(Activation.RELU)
                        .build())
                .layer(1, new OutputLayer.Builder(LossFunction.NEGATIVELOGLIKELIHOOD)
                        .weightInit(WeightInit.XAVIER)
                        .activation(Activation.SOFTMAX)
                        .nIn(numHiddenNodes).nOut(numOutputs).build())
                .build();
		MultiLayerNetwork model = new MultiLayerNetwork(conf);
        model.init();
        model.setListeners(new ScoreIterationListener(10));
        List<List<String>> tmp = new ArrayList<>();
       
        for(SamplePoint sp:samples.getData()) {
        	List<String> arr = new ArrayList<>();
        	arr.add(String.valueOf(sp.value));
        	for(double d:sp.features) {
        		arr.add(String.valueOf(d));
        	} 
        	tmp.add(arr);
        }
        ListStringSplit input = new ListStringSplit(tmp);
        ListStringRecordReader rr = new ListStringRecordReader();
       try {
    	   rr.initialize(input);
       }catch(Exception e) {
    	   
       }
       
        DataSetIterator trainIter = new RecordReaderDataSetIterator(rr, 1);
        
        for ( int n = 0; n < nEpochs; n++) {
            model.fit( trainIter );
        }

        System.out.println("Evaluate model....");
        Evaluation eval = new Evaluation(numOutputs);
        while(trainIter.hasNext()){
            DataSet t = trainIter.next();
            INDArray features = t.getFeatures();
            INDArray lables = t.getLabels();
            INDArray predicted = model.output(features,false);

            eval.eval(lables, predicted);

        }

        //Print the evaluation statistics
        System.out.println(eval.stats());
		return DP;
	} 
}
