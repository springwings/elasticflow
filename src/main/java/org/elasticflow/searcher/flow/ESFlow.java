package org.elasticflow.searcher.flow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticflow.connect.EsConnector;
import org.elasticflow.field.EFField;
import org.elasticflow.model.searcher.ResponseDataUnit;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.searcher.SearcherFlowSocket;
import org.elasticflow.searcher.handler.Handler;
import org.elasticflow.util.EFException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
			throws EFException {
		PREPARE(false, true);
		boolean releaseConn = false;
		SearcherResult res = new SearcherResult();
		if(!ISLINK())
			return res;
		try{ 
			EsConnector ESC = (EsConnector) GETSOCKET().getConnection(true);
			RestHighLevelClient conn = ESC.getClient();
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
				Map<String, EFField> tmpFields = instanceConfig.getWriteFields();
				for (Map.Entry<String, EFField> e : tmpFields.entrySet()) {
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
			throw new EFException(e);
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
				EFField param = instanceConfig.getWriteFields().get(name);
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
