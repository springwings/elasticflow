package org.elasticflow.ml;

import java.util.Map.Entry;
import java.util.Set;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.EFException;
 

/**
 * Onnx Model Compute
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
public class ModelService extends ComputerFlowSocket {

	public static ModelService getInstance(final ConnectParams connectParams) {
		ModelService o = new ModelService();
		o.initConn(connectParams);
		o.init();
		return o;
	}

	public void init() {
		 
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
				for (Entry<String, Object> k : itr) { 
					 
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
