package org.elasticflow.reader;

import java.lang.reflect.Method;

import org.elasticflow.flow.Socket;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;

/**
 * @param args getInstance function parameters: ConnectParams param, String
 *             L1Seq, String readerFlowhandler,
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

	private static ReaderFlowSocket flowChannel(final ConnectParams connectParams, String L1Seq,
			String readerFlowhandler) {
		String _class_name;
		if (readerFlowhandler != null) {
			_class_name = readerFlowhandler;
		} else {
			_class_name = "org.elasticflow.reader.flow."
					+ Common.changeFirstCase(connectParams.getWhp().getType().name().toLowerCase()) + "Reader";
		}
		try {
			Class<?> clz = Class.forName(_class_name);
			Method m = clz.getMethod("getInstance", ConnectParams.class);
			return (ReaderFlowSocket) m.invoke(null, connectParams);
		} catch (Exception e) {
			if (readerFlowhandler != null) {
				Common.LOG.error("instance {} custom reader flow handler {} not exists!",
						connectParams.getInstanceConfig().getInstanceID(), readerFlowhandler, e);
			} else {
				Common.LOG.error("The reader flow socket type {} configured by {} does not exist!",
						connectParams.getWhp().getType(), connectParams.getInstanceConfig().getInstanceID(), e);
			}
			Common.stopSystem(false);
		}
		return null;
	}
}
