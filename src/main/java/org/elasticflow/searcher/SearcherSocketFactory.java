package org.elasticflow.searcher;

import java.util.HashMap;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.flow.Socket;
import org.elasticflow.param.warehouse.WarehouseNosqlParam;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.searcher.flow.ESFlow;
import org.elasticflow.searcher.flow.MysqlFlow;
import org.elasticflow.searcher.flow.SolrFlow;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:24
 */
public class SearcherSocketFactory implements Socket<SearcherFlowSocket>{

	private static SearcherSocketFactory o = new SearcherSocketFactory();
	
	public static SearcherFlowSocket getInstance(Object... args) {
		return o.getSocket(args);
	} 
	 
	/** 
	 * @param args final WarehouseParam param, final InstanceConfig instanceConfig,
			String L1seq
	 * @return
	 */
	@Override
	public SearcherFlowSocket getSocket(Object... args) {
		WarehouseParam param = (WarehouseParam) args[0];
		InstanceConfig instanceConfig = (InstanceConfig) args[1];
		String L1seq = (String) args[2];
		if (param instanceof WarehouseNosqlParam) {
			return getNosqlFlowSocket(param, instanceConfig, L1seq);
		} else {
			return getSqlFlowSocket(param, instanceConfig, L1seq);
		}
	} 

	private static SearcherFlowSocket getNosqlFlowSocket(WarehouseParam params, InstanceConfig instanceConfig, String L1seq) {
		HashMap<String, Object> connectParams = params.getConnectParams(L1seq);
		connectParams.put("instanceConfig", instanceConfig);
		connectParams.put("handler", params.getHandler()); 
		SearcherFlowSocket searcher = null;
		switch (params.getType()) {
		case ES:
			searcher = ESFlow.getInstance(connectParams);
			break;
		case SOLR:
			searcher = SolrFlow.getInstance(connectParams);
			break;
		default:
			break;
		}
		return searcher;
	}

	private static SearcherFlowSocket getSqlFlowSocket(WarehouseParam params, InstanceConfig instanceConfig, String L1seq) {
		HashMap<String, Object> connectParams = params.getConnectParams(L1seq);
		SearcherFlowSocket searcher = null;
		switch (params.getType()) {
		case MYSQL:
			searcher = MysqlFlow.getInstance(connectParams);
			break;
		default:
			break;
		}
		return searcher;
	} 
}
