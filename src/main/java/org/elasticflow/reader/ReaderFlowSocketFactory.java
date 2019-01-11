package org.elasticflow.reader;

import java.lang.reflect.Method;

import org.elasticflow.flow.Socket;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.flow.FileFlow;
import org.elasticflow.reader.flow.HbaseFlow;
import org.elasticflow.reader.flow.MysqlFlow;
import org.elasticflow.reader.flow.OracleFlow;
import org.elasticflow.util.Common;

/**
 * @param args getInstance function parameters:ConnectParams param, String
 *             L1Seq,String handler
 * @author chengwen
 * @version 2.0
 * @date 2019-01-09 11:32
 */
public final class ReaderFlowSocketFactory implements Socket<ReaderFlowSocket> {

	private static ReaderFlowSocketFactory o = new ReaderFlowSocketFactory();

	public static ReaderFlowSocket getInstance(Object... args) {
		return o.getSocket(args);
	}

	@Override
	public ReaderFlowSocket getSocket(Object... args) {
		ConnectParams connectParams = (ConnectParams) args[0];
		String L1Seq = (String) args[1];
		String handler = (String) args[2];
		return flowChannel(connectParams, L1Seq, handler);
	}

	private static ReaderFlowSocket flowChannel(final ConnectParams connectParams, String L1Seq, String handler) { 
		if (handler != null) {
			try {
				Class<?> clz = Class.forName("org.elasticflow.reader.handler." + handler);
				Method m = clz.getMethod("getInstance", ConnectParams.class);
				return (ReaderFlowSocket) m.invoke(null, connectParams);
			} catch (Exception e) {
				Common.LOG.error("getNoSqlFlow Exception!", e);
			} 
		}
		switch (connectParams.getWhp().getType()) {
		case MYSQL:
			return MysqlFlow.getInstance(connectParams); 
		case ORACLE:
			return OracleFlow.getInstance(connectParams); 
		case HBASE:
			return HbaseFlow.getInstance(connectParams);
		case FILE: 
			return FileFlow.getInstance(connectParams);
		default:
			Common.LOG.error("WriterFlowSocket Connect Type "+connectParams.getWhp().getType()+" Not Support!");
			return null;
		} 
	}  
}
