package org.elasticflow.searcher.flow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticflow.connect.ESConnector;
import org.elasticflow.field.RiverField;
import org.elasticflow.model.searcher.ResponseDataUnit;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.searcher.SearcherFlowSocket;
import org.elasticflow.searcher.handler.Handler;
import org.elasticflow.util.FNException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import com.alibaba.fastjson.JSON;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:23
 */
public final class ESFlow extends SearcherFlowSocket {  
	 
	public static ESFlow getInstance(ConnectParams connectParams) {
		ESFlow o = new ESFlow();
		o.INIT(connectParams);
		return o;
	} 
 

	@SuppressWarnings("unchecked")
	@Override
	public SearcherResult Search(SearcherModel<?, ?, ?> fq, String instance,Handler handler)
			throws FNException {
		PREPARE(false, true);
		boolean releaseConn = false;
		SearcherResult res = new SearcherResult();
		if(!ISLINK())
			return res;
		try{ 
			ESConnector ESC = (ESConnector) GETSOCKET().getConnection(true);
			Client conn = ESC.getClient();
			int start = fq.getStart();
			int count = fq.getCount(); 
			List<SortBuilder<?>> sortFields = (List<SortBuilder<?>>) fq.getSortinfo();
			QueryBuilder qb = (QueryBuilder) fq.getQuery();
			List<AggregationBuilder> facetBuilders = (List<AggregationBuilder>) fq
					.getFacetsConfig(); 
			
			List<String> returnFields = new ArrayList<String>();
			if (fq.getFl().length() > 0) {
				for (String s : fq.getFl().split(",")) {
					returnFields.add(s);
				}
			} else {
				Map<String, RiverField> tmpFields = instanceConfig.getWriteFields();
				for (Map.Entry<String, RiverField> e : tmpFields.entrySet()) {
					if (e.getValue().getStored().equalsIgnoreCase("true"))
						returnFields.add(e.getKey());
				}
			} 
			SearchResponse response = getSearchResponse(conn,qb, returnFields, instance,
					start, count, sortFields, facetBuilders, fq,res);
			if(handler==null) {
				addResult(res,response,fq,returnFields);
			}else {
				handler.Handle(res,response,fq,instanceConfig,returnFields);
			} 
		}catch(Exception e){ 
			releaseConn = true;
			throw e;
		}finally{
			REALEASE(false,releaseConn); 
		} 
		return res;
	} 
	
	private void addResult(SearcherResult res,SearchResponse response,SearcherModel<?, ?, ?> fq,List<String> returnFields) {
		SearchHits searchHits = response.getHits();
		res.setTotalHit((int) searchHits.getTotalHits());  
		SearchHit[] hits = searchHits.getHits();  
		 
		for (SearchHit h:hits) {
			Map<String, DocumentField> fieldMap = h.getFields(); 
			ResponseDataUnit u = ResponseDataUnit.getInstance();
			for (Map.Entry<String, DocumentField> e : fieldMap.entrySet()) {
				String name = e.getKey();
				RiverField param = instanceConfig.getWriteFields().get(name);
				DocumentField v = e.getValue();  
				if (param!=null && param.getSeparator() != null) { 
					u.addObject(name, v.getValues());
				} else if(returnFields.contains(name)) {
					u.addObject(name, v.getValue());
				}
			}
			if(fq.isShowQueryInfo()){ 
				u.addObject("_SCORE", h.getExplanation().getValue()); 
				u.addObject("_EXPLAINS", h.getExplanation().toString().replace("", ""));
			}
			res.getUnitSet().add(u);  
		} 
		 
		if (fq.getFacetSearchParams() != null
				&& response.getAggregations() != null) {  
			res.setFacetInfo(JSON.parseObject(response.toString()).get("aggregations"));
		} 
	}
	 
	private SearchResponse getSearchResponse(Client conn,QueryBuilder qb,
			List<String> returnFields, String instance, int start, int count,
			List<SortBuilder<?>> sortFields,
			List<AggregationBuilder> facetBuilders,SearcherModel<?, ?, ?> fq,SearcherResult res) {
		SearchRequestBuilder request = conn.prepareSearch(instance).setPreference("_replica_first");
		request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		request.setQuery(qb);
		request.setSize(count);
		request.setFrom(start);

		if (sortFields != null)
			for (SortBuilder<?> s : sortFields) {
				request.addSort(s);
			}
 
		if (facetBuilders != null)
			for (AggregationBuilder facet : facetBuilders) {
				request.addAggregation(facet);
			} 
		
		request.storedFields(returnFields.toArray(new String[returnFields.size()])); 
	 
		if (fq.isShowQueryInfo()) { 
			res.setQueryDetail(JSON.parse(request.toString()));
			request.setExplain(true); 
		} 
		SearchResponse response = request.execute().actionGet();
		return response;
	}
 
}
