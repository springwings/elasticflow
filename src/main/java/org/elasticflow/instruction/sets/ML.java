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
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Machine learning running Instruction sets
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
public class ML extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("ML");

	/**
	 * @param args parameter order is:String contextId, String types, String
	 *             instance, DataPage pageData
	 * @throws Exception
	 */
	public static DataPage compute(Context context, Object[] args) {
		DataPage res = new DataPage();
		if (!isValid(4, args)) {
			log.error("Compute parameter not match!");
			return res;
		}
		DataPage dp = (DataPage) args[3];

		if (dp.size() == 0)
			return res;

		DataSetReader DSReader = new DataSetReader();
		DSReader.init(dp);
		if (DSReader.status()) {
			try {
				long start = System.currentTimeMillis();
				int dataNums = DSReader.getDataNums(); 
				res = context.getComputer().predict(context, DSReader);
				context.getComputer().flowState
						.setLoad((long) ((dataNums * 1000) / (start - context.getComputer().lastGetPageTime)));
				context.getComputer().lastGetPageTime = start;
				if (res.getData().size() > 0)
					context.getComputer().flowState
							.setPerformance((long) ((dataNums * 1000) / (System.currentTimeMillis() - start + 1e-3)));
				context.getComputer().flowState.incrementCurrentTimeProcess(dataNums);
			} catch (EFException e) {
				log.error("batch Compute Exception", e);
				Common.processErrorLevel(e);
			} finally {
				DSReader.close();
			}
		}
		return res;
	}
}
