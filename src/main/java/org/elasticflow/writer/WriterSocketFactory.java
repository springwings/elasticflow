package org.elasticflow.writer;

import java.lang.reflect.Method;

import org.elasticflow.flow.Socket;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.writer.flow.ESFlow;
import org.elasticflow.writer.flow.HBaseFlow;
import org.elasticflow.writer.flow.MysqlFlow;
import org.elasticflow.writer.flow.SolrFlow;

/**
 * @param args getInstance function parameters:WarehouseParam param, String seq,String handler
 * @author chengwen
 *  @version 1.0
 */
public class WriterSocketFactory implements Socket<WriterFlowSocket>{ 
	
	private static WriterSocketFactory o = new WriterSocketFactory();
	
	public static WriterFlowSocket getInstance(Object... args) {
		return o.getSocket(args);
	}
	
 
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
			default:
				Common.LOG.error("WriterFlowSocket Connect Type "+connectParams.getWhp().getType()+" Not Support!");
				return null;
		}  
	} 
}
