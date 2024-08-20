package org.elasticflow.searcher;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticflow.config.GlobalParam.DATA_SOURCE_TYPE;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.flow.Flow;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.searcher.ResponseDataUnit;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.searcher.handler.SearcherHandler;
import org.elasticflow.util.EFException;

/**
 * Searcher Flow Socket
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
	public void initFlow() {}
	
	@Override
	public void releaseCall() {}
	
	
	/**
	 * release searcher flow
	 */
	@Override
	public void release() { 
		releaseConn(isConnMonopoly,isDiffEndType); 
	}

	/**
	 * Searcher main entrance
	 * 
	 * @param query
	 * @param instance
	 * @param handler
	 * @return
	 * @throws EFException
	 */
	public abstract void Search(SearcherModel<?> searcherModel, String instance, SearcherHandler handler,
			EFResponse efResponse) throws EFException;

	public DATA_SOURCE_TYPE getType() {
		return connectParams.getWhp().getType();
	}
	
	public void formatResult(SearcherResult data,EFResponse response) {
		Map<String, Object> contentMap = new LinkedHashMap<String, Object>();
		contentMap.put("total", data.getTotalHit()); 
		List<Object> objList = new ArrayList<Object>();
		for (ResponseDataUnit unit : data.getUnitSet()) {
			objList.add(unit.getContent());
		}
		contentMap.put("lists", objList); 			
		if (data.getFacetInfo()!=null)
			contentMap.put("facet", data.getFacetInfo());  
		if (data.getQueryDetail() != null)
			contentMap.put("query", data.getQueryDetail()); 
		if (data.getExplainInfo() != null)
			contentMap.put("explain", data.getExplainInfo()); 
		if (data.getStat() != null)
			contentMap.put("__STATS", data.getStat()); 
		if(data.isSuccess()==false) 
			response.setStatus(data.getErrorInfo(), RESPONSE_STATUS.ParameterErr); 
		response.setPayload(contentMap);
	}
}
