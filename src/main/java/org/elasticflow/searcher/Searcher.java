package org.elasticflow.searcher;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.RiverRequest;
import org.elasticflow.model.ResponseState;
import org.elasticflow.model.searcher.ResponseDataUnit;
import org.elasticflow.model.searcher.SearcherESModel;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.model.searcher.SearcherSolrModel;
import org.elasticflow.searcher.handler.Handler;
import org.elasticflow.util.SearchParamUtil;

/**
 * provide search service
 * @author chengwen
 * @version 2.0
 * @date 2018-11-01 17:01
 */
public class Searcher {
	private final static Logger log = LoggerFactory.getLogger(Searcher.class);
	private SearcherFlowSocket searcherFlowSocket;
	private InstanceConfig instanceConfig;
	private String instanceName;
	private Handler handler;

	public static Searcher getInstance(String instanceName,
			InstanceConfig instanceConfig, SearcherFlowSocket searcher) {
		return new Searcher(instanceName, instanceConfig, searcher);
	}

	private Searcher(String instanceName, InstanceConfig instanceConfig,
			SearcherFlowSocket searcherFlowSocket) {
		this.instanceName = instanceName;
		this.searcherFlowSocket = searcherFlowSocket;
		this.instanceConfig = instanceConfig;
		try {
			if(instanceConfig.getPipeParams().getSearcherHandler()!=null) {
				this.handler = (Handler) Class.forName(instanceConfig.getPipeParams().getSearcherHandler()).newInstance();
			}
		}catch(Exception e){
			log.error("FNSearcher Handler Exception",e);
		}
	}

	public ResponseState startSearch(RiverRequest rq) {
		ResponseState response = ResponseState.getInstance();
		response.setInstance(instanceName);
		/** check validation */
		if (!rq.isValid()) {
			response.setError_info("handle is null!");
			return response;
		}

		if (this.searcherFlowSocket == null) {
			response.setError_info("searcher is null!");
			response.setParams(rq.getParams(), null);
			return response;
		}
		response.setParams(rq.getParams(), instanceConfig); 
		SearcherModel<?, ?, ?> searcherModel = null;
		switch (this.searcherFlowSocket.getType()) {
		case ES:
			searcherModel = SearcherESModel.getInstance(rq,instanceConfig);
			SearchParamUtil.normalParam(rq, searcherModel,instanceConfig);
			break;
		case SOLR:
			searcherModel = SearcherSolrModel.getInstance(rq,instanceConfig);
			SearchParamUtil.normalParam(rq, searcherModel,instanceConfig);
			break; 
		default:
			response.setError_info("Not Support Searcher Type!");
			return response; 
		}  
		try {
			if(rq.hasErrors()) {
				response.setError_info(rq.getErrors());
			}else {
				response.setPayload(formatResult(this.searcherFlowSocket.Search(searcherModel, instanceName,handler)));
			} 
		} catch (Exception e) {
			response.setError_info("search parameter may be error!");
			log.error(rq.getPipe()+" FNResponse Exception,", e);
		}
		return response;
	}  
	
	private static Map<String, Object> formatResult(SearcherResult data) {
		Map<String, Object> contentMap = new LinkedHashMap<String, Object>();
		contentMap.put("total", data.getTotalHit()); 
		List<Object> objList = new ArrayList<Object>();
		for (ResponseDataUnit unit : data.getUnitSet()) {
			objList.add(unit.getContent());
		}
		if (objList.size() > 0)
			contentMap.put("list", objList); 
		if (data.getFacetInfo()!=null)
			contentMap.put("facet", data.getFacetInfo());  
		if (data.getQueryDetail() != null)
			contentMap.put("query", data.getQueryDetail()); 
		if (data.getExplainInfo() != null)
			contentMap.put("explain", data.getExplainInfo()); 
		return contentMap;
	}
} 