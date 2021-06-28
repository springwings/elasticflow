package org.elasticflow.searcher;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.flow.Socket;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.searcher.flow.ESFlow;
import org.elasticflow.searcher.flow.MysqlFlow;
import org.elasticflow.searcher.flow.SolrFlow;
import org.elasticflow.util.Common;

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
		ConnectParams param = (ConnectParams) args[0];
		InstanceConfig instanceConfig = (InstanceConfig) args[1];
		String L1seq = (String) args[2];
		return getFlowSocket(param, instanceConfig, L1seq);
	} 

	private static SearcherFlowSocket getFlowSocket(ConnectParams connectParams, InstanceConfig instanceConfig, String L1seq) {
		connectParams.setInstanceConfig(instanceConfig);
		switch (connectParams.getWhp().getType()) {
		case ES:
			return ESFlow.getInstance(connectParams); 
		case SOLR:
			return SolrFlow.getInstance(connectParams); 
		case MYSQL:
			return MysqlFlow.getInstance(connectParams); 
		default:
			Common.LOG.error("SearcherSocket Connect Type "+connectParams.getWhp().getType()+" Not Support!");
			return null;
		} 
	}
 
}
