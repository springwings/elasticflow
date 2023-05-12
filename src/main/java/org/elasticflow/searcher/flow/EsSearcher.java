package org.elasticflow.searcher.flow;

import java.io.IOException;
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
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:23
 */
public final class EsSearcher extends SearcherFlowSocket {  
	 
	public static EsSearcher getInstance(ConnectParams connectParams) {
		EsSearcher o = new EsSearcher();
		o.initConn(connectParams);
		return o;
	} 
 

	@SuppressWarnings("unchecked")
	@Override
	public SearcherResult Search(SearcherModel<?, ?, ?> query, String instance,SearcherHandler handler)
			throws EFException {
		PREPARE(false, true, false);
		boolean releaseConn = false;
		SearcherResult res = new SearcherResult();
		if(!ISLINK())
			return res;
		try{ 
			EsConnector ESC = (EsConnector) GETSOCKET().getConnection(END_TYPE.searcher);
			RestHighLevelClient conn = ESC.getClient();
			int start = query.getStart();
			int count = query.getCount(); 
			List<SortBuilder<?>> sortFields = (List<SortBuilder<?>>) query.getSortinfo();
			QueryBuilder qb = (QueryBuilder) query.getQuery();
			List<AggregationBuilder> facetBuilders = (List<AggregationBuilder>) query
					.getFacetsConfig(); 
			
			List<String> returnFields = new ArrayList<String>();
			if (query.getFl()!=null) {
				for (String s : query.getFl().split(",")) {
					returnFields.add(s);
				}
			} else {
				Map<String, EFField> tmpFields = instanceConfig.getSearchFields();
				for (Map.Entry<String, EFField> e : tmpFields.entrySet()) {
					if (e.getValue().getStored().equalsIgnoreCase("true"))
						returnFields.add(e.getKey());
				}
			} 
			SearchResponse response = getSearchResponse(conn,qb, returnFields, instance,
					start, count, sortFields, facetBuilders, query,res);
			if(handler==null) {
				addResult(res,response,query,returnFields);
			}else {
				handler.Handle(res,response,query,instanceConfig,returnFields);
			} 
			if(query.isShowStats()) {   
				MainResponse tmp = conn.info(RequestOptions.DEFAULT); 
				JSONObject jo = new JSONObject();
				jo.put("clusterName", tmp.getClusterName());
				jo.put("nodeName", tmp.getNodeName());
				jo.put("version", tmp.getVersion());
				jo.put("tagline", tmp.getTagline());
				res.setStat(jo);
			}
		}catch(Exception e){ 
			releaseConn = true;
			throw Common.getException(e);
		}finally{
			REALEASE(false,releaseConn); 
		} 
		return res;
	} 
	
	private void addResult(SearcherResult res,SearchResponse response,SearcherModel<?, ?, ?> fq,List<String> returnFields) {
		SearchHits searchHits = response.getHits();
		res.setTotalHit(searchHits.getTotalHits().value);  
		SearchHit[] hits = searchHits.getHits();  
		 
		for (SearchHit h:hits) {
			Map<String, DocumentField> fieldMap = h.getFields(); 
			ResponseDataUnit u = ResponseDataUnit.getInstance();
			for (Map.Entry<String, DocumentField> e : fieldMap.entrySet()) {
				String name = e.getKey();
				EFField param = instanceConfig.getSearchFields().get(name);
				DocumentField v = e.getValue();  
				if (param!=null && param.getSeparator() != null) { 
					u.addObject(name, v.getValues());
				} else if(returnFields.contains(name)) {
					u.addObject(name, v.getValue());
				}
			} 
			if(fq.isShowQueryInfo()){  
				u.addObject(GlobalParam.RESPONSE_SCORE, h.getExplanation().getValue()); 
				u.addObject(GlobalParam.RESPONSE_EXPLAINS, h.getExplanation().toString().replace("", ""));
			}
			res.getUnitSet().add(u);  
		} 
		 
		if (fq.getFacetSearchParams() != null
				&& response.getAggregations() != null) {  
			res.setFacetInfo(JSON.parseObject(response.toString()).get("aggregations"));
		} 
	}
	 
	private SearchResponse getSearchResponse(RestHighLevelClient conn,QueryBuilder qb,
			List<String> returnFields, String instance, int start, int count,
			List<SortBuilder<?>> sortFields,
			List<AggregationBuilder> facetBuilders,SearcherModel<?, ?, ?> fq,SearcherResult res) throws IOException {
		SearchRequest searchRequest = new SearchRequest(instance); 
		SearchSourceBuilder _SSB = new SearchSourceBuilder(); 
		_SSB.query(qb);
		_SSB.size(count);
		_SSB.from(start);

		if (sortFields != null)
			for (SortBuilder<?> s : sortFields) {
				_SSB.sort(s);
			}
 
		if (facetBuilders != null)
			for (AggregationBuilder facet : facetBuilders) {
				_SSB.aggregation(facet);
			}  
		_SSB.storedFields(returnFields);  
		if (fq.isShowQueryInfo()) { 
			res.setQueryDetail(JSON.parse(_SSB.toString()));
			_SSB.explain(true); 
		} 
		searchRequest.source(_SSB); 
		SearchResponse response = conn.search(searchRequest, RequestOptions.DEFAULT); 
		return response;
	}
 
}
