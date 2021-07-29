/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.instruction.sets;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.instruction.Context;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
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
		String types = String.valueOf(args[1]);
		String instance = String.valueOf(args[2]);
		DataPage dp = (DataPage) args[3];

		if (dp.size() == 0)
			return res;

		DataSetReader DSReader = new DataSetReader();
		DSReader.init(dp);
		long start = Common.getNow();
		int num = DSReader.getDataNums();
		if (DSReader.status()) {
			try { 
				if(context.getInstanceConfig().getComputeParams().getStage().equals(GlobalParam.COMPUTER_STAGE.PREDICT.name())) {
					res = context.getComputer().predict(context, DSReader);
				}else if(context.getInstanceConfig().getComputeParams().getStage().equals(GlobalParam.COMPUTER_STAGE.TRAIN.name()))  {
					res = context.getComputer().train(context, DSReader, context.getInstanceConfig().getWriteFields());
				} 
				log.info(Common.formatLog("onepage", " -- " + types + " compute onepage ", instance,
						context.getInstanceConfig().getComputeParams().getAlgorithm(), "", num,
						DSReader.getDataBoundary(), DSReader.getScanStamp(), Common.getNow() - start, ""));
			} catch (Exception e) {
				log.error("batch Compute Exception", e);
			} finally {
				DSReader.close();
			}
		}
		return res;
	} 
}
