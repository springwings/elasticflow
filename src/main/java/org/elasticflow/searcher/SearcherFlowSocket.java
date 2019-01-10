package org.elasticflow.searcher;

import org.elasticflow.config.GlobalParam.DATA_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.flow.Flow;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.searcher.handler.Handler;
import org.elasticflow.util.FNException;

/**
 * 
 * @author chengwen
 * @version 3.0
 * @date 2019-01-09 15:02
 */
public abstract class SearcherFlowSocket extends Flow {
 
	protected InstanceConfig instanceConfig;

	@Override
	public void INIT(ConnectParams connectParams) {
		this.connectParams = connectParams;
		this.poolName = connectParams.getWhp().getPoolName(connectParams.getL1Seq());
		this.instanceConfig = connectParams.getInstanceConfig(); 
	} 

	public abstract SearcherResult Search(SearcherModel<?, ?, ?> query, String instance, Handler handler)
			throws FNException; 

	public DATA_TYPE getType() {
		return connectParams.getWhp().getType();
	}

}
