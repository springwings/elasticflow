package org.elasticflow.computer;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.ml.Algorithm;
import org.elasticflow.model.Page;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.EFRequest;
import org.elasticflow.model.computer.SamplePoint;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory; 

/**
 * Provide computing stream service
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class Computer {
	private final static Logger log = LoggerFactory.getLogger(Computer.class);
	private InstanceConfig instanceConfig;
	private String instanceName; 
	private ReaderFlowSocket readerFlowSocket;
	private Object model;
	private Algorithm algorithm;
	private String usage;
	
	public static Computer getInstance(String instanceName,InstanceConfig instanceConfig,ReaderFlowSocket readerFlowSocket) {
		return new Computer(instanceName, instanceConfig, readerFlowSocket);
	}
	
	private Computer(String instanceName, InstanceConfig instanceConfig,ReaderFlowSocket readerFlowSocket) {
		this.instanceConfig = instanceConfig;
		this.instanceName = instanceName;
		this.readerFlowSocket = readerFlowSocket;
	}
	
	public EFResponse startCompute(EFRequest rq) {
		EFResponse response = EFResponse.getInstance();
		response.setInstance(instanceName); 
		if(model==null) {
			log.info("start loading model..."); 
			String table = Common.getStoreName(instanceName, Common.getStoreId(instanceName,"",true)); 
			Page JP = new Page();
			JP.setAdditional("select model,remark from "+table);
			JP.setTransField(instanceConfig.getWriteFields());
			DataPage dp = readerFlowSocket.getPageData(JP,instanceConfig.getPipeParams().getReadPageSize());
			DataSetReader DSReader = new DataSetReader();
			DSReader.init(dp);
			while (DSReader.nextLine()) {
				PipeDataUnit pd = DSReader.getLineData();
				model = pd.getData().get("model");
				usage = String.valueOf(pd.getData().get("remark"));
			} 
			log.info("computer model is ready!");
			try {
				Class<?> clz = Class.forName("org.elasticflow.ml.algorithm."+instanceConfig.getComputeParams().getAlgorithm()); 
				algorithm = (Algorithm) clz.newInstance();
				algorithm.loadModel(model);
			}catch (Exception e) {
				log.error("load algorithm Exception",e);
			} 
		}   
		if(rq.getParam("data")!=null) {
			String[] dt = String.valueOf(rq.getParam("data")).split(",");
			SamplePoint point = new SamplePoint(dt.length);
			for(int i=0;i<dt.length;i++) {
				point.features[i] = Double.valueOf(dt[i]);
			}  
			response.setPayload(algorithm.predict(point));
		} 
		response.setInfo(usage); 
		return response;
	}
}
