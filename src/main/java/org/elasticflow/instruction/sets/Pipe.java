/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.instruction.sets;

import org.elasticflow.instruction.Context;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.model.Page;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.ReaderState;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ETYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class Pipe extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("Pipe");

	/**
	 * @param args
	 *            parameter order is: String mainName, String storeId
	 */
	public static void create(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("Pipe create parameter not match!");
			return;
		}

	}

	public static void remove(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("Pipe remove parameter not match!");
			return;
		}

	}
	
	/**
	 * @param args
	 *            parameter order is: Page page,ReaderFlowSocket RFS
	 */
	public static DataPage fetchPage(Context context, Object[] args) { 
		if (!isValid(2, args)) {
			log.error("fetchPage parameter not match!");
			return null;
		}
		Page page = (Page) args[0]; 
		ReaderFlowSocket RFS = (ReaderFlowSocket) args[1]; 
		DataPage tmp = (DataPage) RFS.getPageData(page,context.getInstanceConfig().getPipeParams().getReadPageSize());
		return (DataPage) tmp.clone();
	} 

	/**
	 * @param args
	 *            parameter order is: String id, String instance, String storeId,
	 *            String L2seq, DataPage pageData, String info, boolean isUpdate,
	 *            boolean monopoly
	 * @throws Exception
	 */
	public static ReaderState writeDataSet(Context context, Object[] args) throws Exception {
		ReaderState rstate = new ReaderState();
		if (!isValid(8, args)) {
			log.error("writeDataSet parameter not match!");
			return rstate;
		}
		String id = String.valueOf(args[0]);
		String instance = String.valueOf(args[1]);
		String storeId = String.valueOf(args[2]);
		String L2seq = String.valueOf(args[3]);
		DataPage pageData = (DataPage) args[4];
		String info = String.valueOf(args[5]);
		boolean isUpdate = (boolean) args[6];
		boolean monopoly = (boolean) args[7];

		if (pageData.size() == 0)
			return rstate;
		DataSetReader DSReader = new DataSetReader();
		DSReader.init(pageData);
		long start = Common.getNow();
		int num = 0;
		if (DSReader.status()) {
			context.getWriter().PREPARE(monopoly, false);
			if (!context.getWriter().ISLINK()) {
				rstate.setStatus(false);
				return rstate;
			}
			boolean freeConn = false;
			try {
				while (DSReader.nextLine()) {
					context.getWriter().write(context.getInstanceConfig().getWriterParams(), DSReader.getLineData(),
							context.getInstanceConfig().getWriteFields(), instance, storeId, isUpdate);
					num++;
				}
				rstate.setReaderScanStamp(DSReader.getScanStamp());
				rstate.setCount(num);
				log.info(Common.formatLog("onepage"," -- " + id + " onepage ", instance, storeId, L2seq, num,
						DSReader.getDataBoundary(), DSReader.getScanStamp(), Common.getNow() - start, info));
			} catch (EFException e) {
				if (e.getErrorType().equals(ETYPE.WRITE_POS_NOT_FOUND)) {
					throw e;
				} else {
					freeConn = true;
				}
			} finally {
				DSReader.close();
				context.getWriter().flush();
				context.getWriter().REALEASE(monopoly, freeConn);
			}
		} else {
			rstate.setStatus(false);
		}
		return rstate;
	}
}
