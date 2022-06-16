/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.computer;

import java.lang.reflect.Method;

import org.elasticflow.flow.Socket;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;

/**
 * @param args getInstance function parameters:ConnectParams param
 * @author chengwen
 * @version 2.0
 * @date 2019-01-09 11:32
 */
public final class ComputerFlowSocketFactory implements Socket<ComputerFlowSocket> {
	
	private static ComputerFlowSocketFactory o = new ComputerFlowSocketFactory();

	public static ComputerFlowSocket getInstance(Object... args) {
		return o.getSocket(args);
	}

	@Override
	public ComputerFlowSocket getSocket(Object... args) { 
		return flowChannel((ConnectParams) args[0]);
	}

	private static ComputerFlowSocket flowChannel(final ConnectParams connectParams) { 
		try {
			Class<?> clz = Class.forName(connectParams.getInstanceConfig().getComputeParams().getAlgorithm());
			Method m = clz.getMethod("getInstance", ConnectParams.class);
			return (ComputerFlowSocket) m.invoke(null, connectParams);
		} catch (Exception e) {
			Common.LOG.error("the "+connectParams.getWhp().getType()+" ComputerFlowSocket does not exist!",e);
		}  
		return null;
	}  

}
