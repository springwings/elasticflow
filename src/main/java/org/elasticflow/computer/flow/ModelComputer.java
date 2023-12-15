package org.elasticflow.computer.flow;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.model.DataSetReader;
import org.elasticflow.util.EFException;
 

/**
 * Onnx Model Compute
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
public class ModelComputer extends ComputerFlowSocket {

	public static ModelComputer getInstance(final ConnectParams connectParams) {
		ModelComputer o = new ModelComputer();
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
				//wait to implement
			}
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, DSR.getScanStamp());
			this.dataPage.putData(this.dataUnit);
			this.dataPage.putDataBoundary(DSR.getDataBoundary());
		}
		return this.dataPage;
	}

}
