package org.elasticflow.searcher.flow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.connection.EsConnector;
import org.elasticflow.field.EFField;
import org.elasticflow.model.searcher.ResponseDataUnit;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.searcher.SearcherFlowSocket;
import org.elasticflow.searcher.handler.SearcherHandler;
import org.elasticflow.searcher.parser.ESQueryParser;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * Main Run Class for Searcher
 * 
 * @author chengwen
 * @version 5.x
 * @date 2018-10-26 09:23
 * @modify 2023-05-26 09:23
 */
public final class EsSearcher extends SearcherFlowSocket {

	public static EsSearcher getInstance(ConnectParams connectParams) {
		EsSearcher o = new EsSearcher();
		o.initConn(connectParams);
		return o;
	}
	   
	@Override
	public SearcherResult Search(SearcherModel<?, ?> searcherModel, String instance, SearcherHandler handler)
			throws EFException {
		PREPARE(false, true, false);
		boolean releaseConn = false;
		SearcherResult res = new SearcherResult();
		if (!ISLINK())
			return res;
		EsConnector ESC = null;
		try {
			ESC = (EsConnector) GETSOCKET().getConnection(END_TYPE.searcher);
			RestHighLevelClient conn = ESC.getClient();  
			List<String> returnFields = new ArrayList<String>();
			if (searcherModel.getFl() != null) {
				for (String s : searcherModel.getFl().split(",")) {
					returnFields.add(s);
				}
			} else {
				Map<String, EFField> tmpFields = instanceConfig.getSearchFields();
				for (Map.Entry<String, EFField> e : tmpFields.entrySet()) {
					if (e.getValue().getStored().equalsIgnoreCase("true"))
						returnFields.add(e.getKey());
				}
			}
			SearchResponse response = getSearchResponse(conn, searcherModel, returnFields, instance,res);
			if (handler == null) {
				addResult(res, response, searcherModel, returnFields);
			} else {
				handler.Handle(res, response, searcherModel, instanceConfig, returnFields);
			}
			if (searcherModel.isShowStats()) {
				MainResponse tmp = conn.info(RequestOptions.DEFAULT);
				JSONObject jo = new JSONObject();
				jo.put("clusterName", tmp.getClusterName());
				jo.put("nodeName", tmp.getNodeName());
				jo.put("version", tmp.getVersion());
				jo.put("tagline", tmp.getTagline());
				res.setStat(jo);
			}
		} catch (Exception e) {
			releaseConn = true;
			if(ESC!=null) {
				throw Common.convertException(e,this.poolName);
			}else {
				throw Common.convertException(e);
			}
		} finally {
			REALEASE(false, releaseConn);
		}
		return res;
	}

	private void addResult(SearcherResult res, SearchResponse response, SearcherModel<?, ?> searcherModel,
			List<String> returnFields) {
		SearchHits searchHits = response.getHits();
		res.setTotalHit(searchHits.getTotalHits().value);
		SearchHit[] hits = searchHits.getHits();

		for (SearchHit h : hits) {
			Map<String, DocumentField> fieldMap = h.getFields();
			ResponseDataUnit u = ResponseDataUnit.getInstance();
			for (Map.Entry<String, DocumentField> e : fieldMap.entrySet()) {
				String name = e.getKey();
				EFField param = instanceConfig.getSearchFields().get(name);
				DocumentField v = e.getValue();
				if (param != null && param.getSeparator() != null) {
					u.addObject(name, v.getValues());
				} else if (returnFields.contains(name)) {
					u.addObject(name, v.getValue());
				}
			}
			if (searcherModel.isShowQueryInfo()) {
				u.addObject(GlobalParam.RESPONSE_SCORE, h.getExplanation().getValue());
				u.addObject(GlobalParam.RESPONSE_EXPLAINS, h.getExplanation().toString().replace("", ""));
			}
			res.getUnitSet().add(u);
		}

		if (searcherModel.getFacetSearchParams() != null && response.getAggregations() != null) {
			res.setFacetInfo(JSON.parseObject(response.toString()).get("aggregations"));
		}
	}
 
	/**
	 * Build run statements and execute them
	 * @param conn
	 * @param searcherModel
	 * @param returnFields
	 * @param instance
	 * @param start
	 * @param count
	 * @param sortFields
	 * @param facetBuilders
	 * @param res
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private SearchResponse getSearchResponse(RestHighLevelClient conn, SearcherModel<?, ?> searcherModel,
			List<String> returnFields, String instance,SearcherResult res) throws Exception {
		SearchRequest searchRequest = new SearchRequest(instance);
		ESQueryParser ESP = new ESQueryParser();
		ESP.parseQuery(instanceConfig, searcherModel);
		ESP.parseFilter(instanceConfig, searcherModel);
		ESP.getSSB().size(searcherModel.getCount());
		ESP.getSSB().from(searcherModel.getStart());
		List<SortBuilder<?>> sortFields = (List<SortBuilder<?>>) searcherModel.getSortinfo(); 
		if (sortFields != null)
			for (SortBuilder<?> s : sortFields) {
				ESP.getSSB().sort(s);
			}
		List<AggregationBuilder> facetBuilders = (List<AggregationBuilder>) searcherModel.getFacetsConfig(); 
		if (facetBuilders != null)
			for (AggregationBuilder facet : facetBuilders) {
				ESP.getSSB().aggregation(facet);
			}
		ESP.getSSB().storedFields(returnFields);
		if (searcherModel.isShowQueryInfo()) {
			res.setQueryDetail(JSON.parse(ESP.getSSB().toString()));
			ESP.getSSB().explain(true);
		}
		searchRequest.source(ESP.getSSB());
		SearchResponse response = conn.search(searchRequest, RequestOptions.DEFAULT);
		return response;
	}

}
