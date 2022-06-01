package org.elasticflow.ml;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.instruction.Context;
import org.elasticflow.ml.common.ImageUtil;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.EFException;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.OrtSession.Result;
import ai.onnxruntime.OrtSession.SessionOptions;
import ai.onnxruntime.OrtSession.SessionOptions.OptLevel;

/**
 * Onnx Model Compute
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
public class ModelService extends ComputerFlowSocket {

	private String modelPath = "/VM/model.onnx";

	private OrtEnvironment env = OrtEnvironment.getEnvironment();

	private OrtSession sess;

	private String inputName;

	private final static Logger log = LoggerFactory.getLogger("ModelService");

	public static ModelService getInstance(final ConnectParams connectParams) {
		ModelService o = new ModelService();
		o.initConn(connectParams);
		o.init();
		return o;
	}

	public void init() {
		OrtSession.SessionOptions opts = new SessionOptions();
		try {
			opts.setOptimizationLevel(OptLevel.BASIC_OPT);
			log.info("Loading model from " + modelPath);
			sess = env.createSession(modelPath, opts);
			inputName = sess.getInputNames().iterator().next();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static int pred(float[] probabilities) {
	    float maxVal = Float.NEGATIVE_INFINITY;
	    int idx = 0;
	    for (int i = 0; i < probabilities.length; i++) {
	      if (probabilities[i] > maxVal) {
	        maxVal = probabilities[i];
	        idx = i;
	      }
	    }
	    return idx;
	  }

	@Override
	public DataPage predict(Context context, DataSetReader DSR) throws EFException {
		if (this.computerHandler != null) {
			this.computerHandler.handleData(this, context, DSR);
		} else {
			while (DSR.nextLine()) {
				PipeDataUnit pdu = DSR.getLineData();
				PipeDataUnit u = PipeDataUnit.getInstance();
				Set<Entry<String, Object>> itr = pdu.getData().entrySet();
				int[][][][] testData = ImageUtil.getImgArrays(new String[] {"/VM/test.jpg"},128,256);
				INDArray features = Nd4j.create(testData); 
				for (Entry<String, Object> k : itr) { 
					try (OnnxTensor test = OnnxTensor.createTensor(env, features);
							Result output = sess.run(Collections.singletonMap(inputName, test))) {
						float[][] outputProbs = (float[][]) output.get(0).getValue();
						System.out.println(outputProbs[0]);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				this.dataUnit.add(u);
			}
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, DSR.getScanStamp());
			this.dataPage.putData(this.dataUnit);
			this.dataPage.putDataBoundary(DSR.getDataBoundary());
		}
		return this.dataPage;
	}

}
