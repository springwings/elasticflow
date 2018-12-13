package org.elasticflow.ml.algorithm;

import java.util.Map;

import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.elasticflow.field.RiverField;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.computer.SampleSets;
import org.elasticflow.model.reader.DataPage;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction;

public class Dl4j {
	
	static int seed = 123;
	static int numInputs = 2;
	static int numOutputs = 2;
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
        RecordReader rr = new CSVRecordReader();
	   // rr.initialize(new FileSplit(new File("")));
        DataSetIterator trainIter = new RecordReaderDataSetIterator(rr,50,0,2);
        
        for ( int n = 0; n < nEpochs; n++) {
            model.fit( trainIter );
        }

        
		return DP;
	} 
}
