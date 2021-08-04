package org.elasticflow.computer.handler;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.instruction.Context;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.EFException;

/**
 * user defined Computer data process function
 * @author chengwen
 * @version 2.0
 * @date 2018-12-28 09:27
 */
public abstract class ComputeHandler {
	
	public abstract void handleData(ComputerFlowSocket invokeObject,Context context,DataSetReader dataSetReader) throws EFException;
	
}
