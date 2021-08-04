package org.elasticflow.writer.handler;

import org.elasticflow.instruction.Context;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.util.EFException;

/**
 * Write Handler interface
 * @author chengwen
 * @version 4.0
 * @date 2018-11-14 16:54
 * 
 */
public abstract class WriteHandler{
	
	public abstract DataPage handleData(Context context,DataPage dataPage) throws EFException;
	
}
