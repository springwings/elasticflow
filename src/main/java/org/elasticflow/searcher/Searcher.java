package org.elasticflow.searcher;

import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFRequest;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.searcher.SearcherElasticsearchModel;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherVearchModel;
import org.elasticflow.searcher.handler.SearcherHandler;
import org.elasticflow.util.instance.SearchParamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ElasticFlow search service base model
 * 
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

	public static Searcher getInstance(String instanceName, InstanceConfig instanceConfig,
			SearcherFlowSocket searcher) {
		return new Searcher(instanceName, instanceConfig, searcher);
	}

	private Searcher(String instanceName, InstanceConfig instanceConfig, SearcherFlowSocket searcherFlowSocket) {
		this.instanceName = instanceName;
		this.searcherFlowSocket = searcherFlowSocket;
		this.instanceConfig = instanceConfig;
		try {
			if (instanceConfig.getPipeParams().getCustomSearcher() != null) {
				this.handler = (SearcherHandler) Class.forName(instanceConfig.getPipeParams().getCustomSearcher())
						.getDeclaredConstructor().newInstance();
			}
		} catch (Exception e) {
			log.error("instance {} Searcher Handler exception", instanceName,e);
		}
	}

	public void startSearch(EFRequest efrq, EFResponse efrp) {
		efrp.setInstance(instanceName);
		/** check validation */
		if (!efrq.isValid()) {
			efrp.setStatus("EFRequest is Valid!", RESPONSE_STATUS.ParameterErr);
			return;
		}

		if (this.searcherFlowSocket == null) {
			efrp.setStatus("searcher Flow Socket is null!", RESPONSE_STATUS.CodeException);
			return;
		}
		SearcherModel<?> searcherModel = null;
		switch (this.searcherFlowSocket.getType()) {
		case ELASTICSEARCH:
			searcherModel = SearcherElasticsearchModel.getInstance(efrq, instanceConfig);
			break;
		case VEARCH:
			searcherModel = SearcherVearchModel.getInstance(efrq, instanceConfig);
			break;
		case KAFKA:
			searcherModel = SearcherVearchModel.getInstance(efrq, instanceConfig);
			break;
		default:
			efrp.setStatus("Not Support Searcher Type " + this.searcherFlowSocket.getType(),
					RESPONSE_STATUS.ParameterErr);
			return;
		}
		SearchParamUtil.normalParam(efrq, searcherModel, instanceConfig);
		try {
			if (efrq.hasErrors()) {
				efrp.setStatus(efrq.getErrors(), RESPONSE_STATUS.CodeException);
			} else {
				this.searcherFlowSocket.Search(searcherModel, instanceName, handler, efrp);
			}
		} catch (Exception e) {
			efrp.setStatus(e.getMessage(), RESPONSE_STATUS.Unknown);
		}
	}
}