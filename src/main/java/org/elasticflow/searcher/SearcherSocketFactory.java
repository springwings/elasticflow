package org.elasticflow.searcher;

import java.lang.reflect.Method;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.flow.Socket;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:24
 */
public class SearcherSocketFactory implements Socket<SearcherFlowSocket> {

	private static SearcherSocketFactory o = new SearcherSocketFactory();

	public static SearcherFlowSocket getInstance(Object... args) {
		return o.getSocket(args);
	}

	/**
	 * @param args final WarehouseParam param, final InstanceConfig instanceConfig,
	 *             String L1seq
	 * @return
	 */
	@Override
	public SearcherFlowSocket getSocket(Object... args) {
		ConnectParams param = (ConnectParams) args[0];
		InstanceConfig instanceConfig = (InstanceConfig) args[1];
		String L1seq = (String) args[2];
		return getFlowSocket(param, instanceConfig, L1seq);
	}

	private static SearcherFlowSocket getFlowSocket(ConnectParams connectParams, InstanceConfig instanceConfig,
			String L1seq) {
		connectParams.setInstanceConfig(instanceConfig);
		String _class_name = "org.elasticflow.searcher.flow."
				+ Common.changeFirstCase(connectParams.getWhp().getType().name().toLowerCase()) + "Searcher";
		try {
			Class<?> clz = Class.forName(_class_name);
			Method m = clz.getMethod("getInstance", ConnectParams.class);
			return (SearcherFlowSocket) m.invoke(null, connectParams);
		} catch (Exception e) {
			Common.systemLog("The searcher flow socket type {} configured by {} does not exist!",
					connectParams.getWhp().getType(), connectParams.getInstanceConfig().getInstanceID(), e);
		}
		return null;
	}

}
