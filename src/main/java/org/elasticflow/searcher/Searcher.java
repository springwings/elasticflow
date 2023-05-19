package org.elasticflow.searcher;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFRequest;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.searcher.ResponseDataUnit;
import org.elasticflow.model.searcher.SearcherESModel;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.model.searcher.SearcherVearchModel;
import org.elasticflow.searcher.handler.SearcherHandler;
import org.elasticflow.util.instance.SearchParamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ElasticFlow search service base model
 * @author chengwen
 * @version 2.0
 * @date 2018-11-01 17:01
 */
public class Searcher {
	private final static Logger log = LoggerFactory.getLogger(Searcher.class);
	private SearcherFlowSocket searcherFlowSocket;
	private InstanceConfig instanceConfig;
	private String instanceName;
	private SearcherHandler handler;

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
			if(instanceConfig.getPipeParams().getCustomSearcher()!=null) {
				this.handler = (SearcherHandler) Class.forName(instanceConfig.getPipeParams().getCustomSearcher()).getDeclaredConstructor().newInstance();
			}
		}catch(Exception e){
			log.error("Searcher Handler Exception",e);
		}
	}

	public void startSearch(EFRequest rq,EFResponse response) {
		response.setInstance(instanceName);
		/** check validation */
		if (!rq.isValid()) {
			response.setStatus("EFRequest is Valid!", RESPONSE_STATUS.ParameterErr);
			return ;
		}

		if (this.searcherFlowSocket == null) {
			response.setStatus("searcher Flow Socket is null!",RESPONSE_STATUS.CodeException);
			return ;
		}
		SearcherModel<?, ?> searcherModel = null;
		switch (this.searcherFlowSocket.getType()) {
		case ES:
			searcherModel = SearcherESModel.getInstance(rq,instanceConfig); 
			break; 
		case VEARCH:
			searcherModel = SearcherVearchModel.getInstance(rq,instanceConfig); 
			break; 
		default:
			response.setStatus("Not Support Searcher Type "+this.searcherFlowSocket.getType(),RESPONSE_STATUS.ParameterErr);
			return; 
		}  
		SearchParamUtil.normalParam(rq, searcherModel,instanceConfig); 
		try {
			if(rq.hasErrors()) {
				response.setStatus(rq.getErrors(),RESPONSE_STATUS.CodeException);
			}else {
				formatResult(this.searcherFlowSocket.Search(searcherModel, instanceName,handler),response);
			} 
		} catch (Exception e) {
			response.setStatus("searcher parameters may be wrong!",RESPONSE_STATUS.ParameterErr);
			log.error(rq.getPipe()+" searcher Response Exception,", e);
		} 
	}  
	
	private static void formatResult(SearcherResult data,EFResponse response) {
		Map<String, Object> contentMap = new LinkedHashMap<String, Object>();
		contentMap.put("total", data.getTotalHit()); 
		List<Object> objList = new ArrayList<Object>();
		for (ResponseDataUnit unit : data.getUnitSet()) {
			objList.add(unit.getContent());
		}
		if (objList.size() > 0)
			contentMap.put("lists", objList); 
		if (data.getFacetInfo()!=null)
			contentMap.put("facet", data.getFacetInfo());  
		if (data.getQueryDetail() != null)
			contentMap.put("query", data.getQueryDetail()); 
		if (data.getExplainInfo() != null)
			contentMap.put("explain", data.getExplainInfo()); 
		if (data.getStat() != null)
			contentMap.put("__STATS", data.getStat()); 
		if(data.isSuccess()==false) {
			response.setStatus(data.getErrorInfo(), RESPONSE_STATUS.ParameterErr);
		}
		response.setPayload(contentMap);
	}
} 