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
import org.elasticflow.writer.flow.ESFlow;
import org.elasticflow.writer.flow.HBaseFlow;
import org.elasticflow.writer.flow.MysqlFlow;
import org.elasticflow.writer.flow.Neo4jFlow;
import org.elasticflow.writer.flow.SolrFlow;
import org.elasticflow.writer.flow.VearchFlow;

/**
 * 
 * @author chengwen
 * @version 4.0
 * @date 2018-11-14 16:54
 */
public class WriterSocketFactory implements Socket<WriterFlowSocket>{ 
	
	private static WriterSocketFactory o = new WriterSocketFactory();
	
	public static WriterFlowSocket getInstance(Object... args) {
		return o.getSocket(args);
	}
	
	/**
	 * args getInstance function parameters
	 * WarehouseParam param, String seq,String handler
	 */
	@Override
	public WriterFlowSocket getSocket(Object... args) {
		ConnectParams param = (ConnectParams) args[0];
		String L1seq = (String) args[1];
		String handler = (String) args[2];
		return getFlowSocket(param,L1seq,handler); 
	} 
 
	
	private static WriterFlowSocket getFlowSocket(ConnectParams connectParams,String L1seq,String handler) { 
		if(handler!=null) {
			try {
				Class<?> clz = Class.forName("org.elasticflow.writer.handler."+handler);
				Method m = clz.getMethod("getInstance",ConnectParams.class);
				return (WriterFlowSocket) m.invoke(null,connectParams);
			}catch (Exception e) {
				Common.LOG.error("getNoSqlFlow Exception!",e);
			}  
		}
		switch (connectParams.getWhp().getType()) {
			case ES:
				return ESFlow.getInstance(connectParams);
			case SOLR:
				return SolrFlow.getInstance(connectParams); 
			case HBASE:
				return HBaseFlow.getInstance(connectParams); 
			case MYSQL: 
				return MysqlFlow.getInstance(connectParams); 
			case NEO4J:
				return Neo4jFlow.getInstance(connectParams); 
			case VEARCH:
				return VearchFlow.getInstance(connectParams); 
			default:
				Common.LOG.error("WriterFlowSocket Connect Type "+connectParams.getWhp().getType()+" Not Support!");
				return null;
		}  
	} 
}
