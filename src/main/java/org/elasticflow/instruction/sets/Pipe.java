/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.instruction.sets;

import org.elasticflow.config.GlobalParam.ETYPE;
import org.elasticflow.instruction.Context;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.ReaderState;
import org.elasticflow.model.task.TaskCursor;
import org.elasticflow.model.task.TaskModel;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.reader.model.DataSetReader;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.writer.WriterFlowSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipe operation Instruction sets
 * It is an instruction code,Can only interpret calls
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class Pipe extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("Pipe----");

	/**
	 * @param args
	 *            parameter order is: String instanceId, String storeId
	 */
	public static void create(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("instruction.set.Pipe.create parameter not match!");
			return;
		}

	}

	public static void remove(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("instruction.set.Pipe.remove parameter not match!");
			return;
		}

	}
	
	/**
	 * @param args
	 *            parameter order is: Page page,ReaderFlowSocket RFS
	 * @throws EFException 
	 */
	public static DataPage fetchPage(Context context, Object[] args) throws EFException { 
		if (!isValid(2, args)) {
			log.error("instruction.set.Pipe.fetchPage parameter not match!");
			return null;
		}
		TaskCursor page = (TaskCursor) args[0]; 
		ReaderFlowSocket RFS = (ReaderFlowSocket) args[1]; 
		long start = System.currentTimeMillis();
		DataPage tmp = (DataPage) RFS.getPageData(page,context.getInstanceConfig().getPipeParams().getReadPageSize());	
		RFS.flowStatistic.setLoad((long)((tmp.getData().size()*1000)/(start-RFS.lastGetPageTime)));		
		RFS.lastGetPageTime = start;
		if(tmp.getData().size()>0)
			RFS.flowStatistic.setPerformance((long) ((tmp.getData().size()*1000)/(System.currentTimeMillis()-start+1e-3)));
		RFS.flowStatistic.incrementCurrentTimeProcess(tmp.getData().size());
		return (DataPage) tmp.clone();
	} 

	/**
	 * @param args
	 *            parameter order is: String jobType, String instance, String storeId,
	 *            String L2seq, DataPage pageData, String info, boolean isUpdate,
	 *            boolean monopoly
	 * @throws Exception
	 */
	public static ReaderState writeDataSet(Context context, Object[] args) throws EFException {
		ReaderState rstate = new ReaderState();
		if (!isValid(8, args)) {
			log.error("instruction.set.Pipe.writeDataSet parameter not match!");
			return rstate;
		}
		String jobType = String.valueOf(args[0]);
		String instance = String.valueOf(args[1]);
		String storeId = String.valueOf(args[2]);
		TaskModel task = (TaskModel) args[3];
		DataPage dataPage = (DataPage) args[4];
		String info = String.valueOf(args[5]);
		boolean isUpdate = (boolean) args[6];
		boolean monopoly = (boolean) args[7];
		//Control the release of problematic connections
		boolean freeConn = false;
		
		WriterFlowSocket writer = context.getWriter();
		writer.PREPARE(monopoly, false,true);
		if (!writer.connStatus()) {
			rstate.setStatus(false);
			return rstate;
		}
		if (dataPage.getData().size()==0) {
			//Prevent status from never being submitted
			context.getReader().flush();
			writer.flush(); 
			writer.releaseConn(monopoly, freeConn); 
			return rstate;
		}
		if(writer.getWriteHandler()!=null)
			dataPage = writer.getWriteHandler().handleData(context, dataPage);		
		DataSetReader DSReader = DataSetReader.getInstance(dataPage);
		long start = System.currentTimeMillis();
		int num = 0;
		if (DSReader.status()) {	
			try {
				while (DSReader.nextLine()) { 
					writer.write(context.getInstanceConfig(),
							DSReader.getLineData().virtualWrite(context.getInstanceConfig().getWriteFields()),//write field handler
							instance, storeId, isUpdate);
					num++;
				}
				rstate.setReaderScanStamp(DSReader.getScanStamp());
				rstate.setCount(num);				
				writer.flowStatistic.setLoad((long)((num*1000)/(start-writer.lastGetPageTime+1e-3)));		
				writer.lastGetPageTime = start;
				if(num>0)
					writer.flowStatistic.setPerformance((long) ((num*1000)/(System.currentTimeMillis()-start+1e-3)));
				writer.flowStatistic.incrementCurrentTimeProcess(num);
				context.getReader().flush();
				writer.flush(); 
				log.info(TaskUtil.formatLog("onepage",jobType + " Write", task.getInstanceProcessId(), 
						storeId, task.getL2seq(), num,
						DSReader.getDataBoundary(), DSReader.getScanStamp(), Common.getNow() - start, info));
			} catch (EFException e) {
				if (e.getErrorType()==ETYPE.RESOURCE_ERROR) { 
					freeConn = true;
				}
				throw e;
			} finally { 
				DSReader.close(); 
				writer.releaseConn(monopoly, freeConn); 
			}
		} else {
			writer.releaseConn(monopoly, freeConn); 
			rstate.setStatus(false);
		}
		return rstate;
	}
}
