/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.writer;

import java.lang.reflect.Method;

import org.elasticflow.flow.Socket;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;

/**
 * Writer Socket Factory
 * 
 * @author chengwen
 * @version 4.0
 * @date 2018-11-14 16:54
 */
public class WriterSocketFactory implements Socket<WriterFlowSocket> {

	private static WriterSocketFactory o = new WriterSocketFactory();

	public static WriterFlowSocket getInstance(Object... args) {
		return o.getSocket(args);
	}

	/**
	 * args getInstance function parameters WarehouseParam param, String seq,String
	 * handler
	 */
	@Override
	public WriterFlowSocket getSocket(Object... args) {
		ConnectParams param = (ConnectParams) args[0];
		String L1seq = (String) args[1];
		String handler = (String) args[2];
		return getFlowSocket(param, L1seq, handler);
	}

	private static WriterFlowSocket getFlowSocket(ConnectParams connectParams, String L1seq, String writerFlowhandler) {
		String _class_name;
		if (writerFlowhandler != null) {
			_class_name = writerFlowhandler;
		} else {
			_class_name = "org.elasticflow.writer.flow."
					+ Common.changeFirstCase(connectParams.getWhp().getType().name().toLowerCase()) + "Writer";
		}
		try {
			Class<?> clz = Class.forName(_class_name);
			Method m = clz.getMethod("getInstance", ConnectParams.class);
			return (WriterFlowSocket) m.invoke(null, connectParams);
		} catch (Exception e) {
			if (writerFlowhandler != null) {
				Common.systemLog("instance {} custom writer flow handler {} exception",
						connectParams.getInstanceConfig().getInstanceID(), writerFlowhandler, e);
			} else {
				Common.systemLog("The writer flow socket type {} configured by {} does not exist!",
						connectParams.getWhp().getType(), connectParams.getInstanceConfig().getInstanceID(), e);
			}
			Common.stopSystem(false);
		}
		return null;
	}
}
