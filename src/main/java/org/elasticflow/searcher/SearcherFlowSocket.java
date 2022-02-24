package org.elasticflow.searcher;

import org.elasticflow.config.GlobalParam.DATA_SOURCE_TYPE;
import org.elasticflow.flow.Flow;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.searcher.handler.SearcherHandler;
import org.elasticflow.util.EFException;

/**
 * 
 * @author chengwen
 * @version 3.0
 * @date 2019-01-09 15:02
 */
public abstract class SearcherFlowSocket extends Flow { 

	@Override
	public void initConn(ConnectParams connectParams) {
		this.connectParams = connectParams;
		this.poolName = connectParams.getWhp().getPoolName(connectParams.getL1Seq());
		this.instanceConfig = connectParams.getInstanceConfig();
	} 
	
	@Override
	public void initFlow() {
		//auto invoke in flow prepare
	}

	public abstract SearcherResult Search(SearcherModel<?, ?, ?> query, String instance, SearcherHandler handler)
			throws EFException; 

	public DATA_SOURCE_TYPE getType() {
		return connectParams.getWhp().getType();
	}

}
