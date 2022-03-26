package org.elasticflow.ml.algorithm;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.EFException;

/**
 * Blank Compute
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
public class BlankService extends ComputerFlowSocket {

	public static BlankService getInstance(final ConnectParams connectParams) {
		BlankService o = new BlankService();
		o.initConn(connectParams);
		return o;
	}

	@Override
	public DataPage predict(Context context, DataSetReader DSR) throws EFException {
		this.dataPage.put(GlobalParam.READER_KEY, context.getInstanceConfig().getComputeParams().getKeyField());
		this.dataPage.put(GlobalParam.READER_SCAN_KEY, context.getInstanceConfig().getComputeParams().getScanField());
		while (DSR.nextLine()) {
			this.dataUnit.add(DSR.getLineData());
		}
		this.dataPage.put(GlobalParam.READER_LAST_STAMP, DSR.getScanStamp());
		this.dataPage.putData(this.dataUnit);
		this.dataPage.putDataBoundary(DSR.getDataBoundary());
		return this.dataPage;
	}
}
