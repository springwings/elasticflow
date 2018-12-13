package org.elasticflow.model.searcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.RiverRequest;
import org.elasticflow.searcher.flow.ESQueryBuilder;
import org.elasticflow.util.SearchParamUtil;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class SearcherESModel implements SearcherModel<QueryBuilder,SortBuilder<?>,AggregationBuilder>{
	private QueryBuilder query;
	private List<SortBuilder<?>> sortinfo;
	private int start = 0;
	private int count = 5;
	Map<String,List<String[]>> facetSearchParams;
	List<AggregationBuilder> facetsConfig = new ArrayList<AggregationBuilder>();
	private Map<String, QueryBuilder> attrQueryMap = new HashMap<String, QueryBuilder>(); 
	private boolean showQueryInfo = false;
	private boolean needCorpfuncCnt = false;
	private boolean cacheRequest = true;
	private Set<Integer> excludeSet;
	private String type;
	private String fl="";
	private String fq="";
	private String facet_ext="";
	private String requesthandler="";
	
	public static SearcherESModel getInstance(RiverRequest request, InstanceConfig instanceConfig) {
		SearcherESModel eq = new SearcherESModel(); 
		eq.setSorts(SearchParamUtil.getSortField(request, instanceConfig));
		eq.setFacetSearchParams(SearchParamUtil.getFacetParams(request, instanceConfig));
		if(request.getParam("facet_ext")!=null){
			eq.setFacet_ext(request.getParams().get("facet_ext"));
		} 
		Map<String, QueryBuilder> attrQueryMap = new HashMap<String, QueryBuilder>();
		BoolQueryBuilder query = ESQueryBuilder.buildBooleanQuery(request,
				instanceConfig, attrQueryMap);
		eq.setQuery(query);
		eq.setAttrQueryMap(attrQueryMap);
		return eq;
	}
 
	
	@Override
	public QueryBuilder getQuery() {
		if (query != null && attrQueryMap.size() > 0)
		{			
			BoolQueryBuilder bQuery = QueryBuilders.boolQuery();
			bQuery.must(query);
			for(QueryBuilder q : attrQueryMap.values()){
				bQuery.must(q);
			}
			return bQuery;
		}
		return query;
	}
	
	@Override
	public void setQuery(QueryBuilder query) {
		this.query = query;
	} 
	
	@Override
	public List<SortBuilder<?>> getSortinfo() {
		return sortinfo;
	}
	public void setSorts(List<SortBuilder<?>> sortinfo) {
		this.sortinfo = sortinfo;
	}
	
	@Override
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	@Override
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	@Override
	public Map<String,List<String[]>> getFacetSearchParams() {
		return facetSearchParams;
	}
	public void setFacetSearchParams(Map<String,List<String[]>> facetSearchParams) {
		this.facetSearchParams = facetSearchParams;
	}
	
	public void setFacet_ext(String facet_ext) {
		this.facet_ext = facet_ext;
	}

	
	@Override
	public List<AggregationBuilder> getFacetsConfig() {
		if (facetSearchParams != null)
		{ 
			for(Map.Entry<String,List<String[]>> e : facetSearchParams.entrySet())
			{	 
				int i=0;
				AggregationBuilder  agg = null ;
				for(String[] strs:e.getValue()) {
					if(i==0) {
						agg = genAgg(strs[0],strs[1],strs[2],true);
					}else {
						((AggregationBuilder) agg).subAggregation(genAgg(strs[0],strs[1],strs[2],false));
					}
					i++; 
				}  
				facetsConfig.add(agg);
			}
		}
		return facetsConfig;
	} 
	
	@Override
	public Map<String, QueryBuilder> getAttrQueryMap() {
		return attrQueryMap;
	}

	public void setAttrQueryMap(Map<String, QueryBuilder> attrQueryMap) {
		this.attrQueryMap = attrQueryMap;
	} 
	
	@Override
	public boolean isShowQueryInfo() {
		return this.showQueryInfo;
	}

	@Override
	public void setShowQueryInfo(boolean isshow) {
		this.showQueryInfo = isshow;
	}

	public boolean isNeedCorpfuncCnt() {
		return needCorpfuncCnt;
	}

	public void setNeedCorpfuncCnt(boolean needCorpfuncCnt) {
		this.needCorpfuncCnt = needCorpfuncCnt;
	}

	public Set<Integer> getExcludeSet() {
		return excludeSet;
	}

	public void setExcludeSet(Set<Integer> excludeSet) {
		this.excludeSet = excludeSet;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	@Override
	public boolean cacheRequest() {
		return cacheRequest;
	}

	public void setCacheRequest(boolean cacheRequest) {
		this.cacheRequest = cacheRequest;
	}

	@Override
	public Map<String, QueryBuilder> getEveryAttrQueriesMap() {
		Map<String, QueryBuilder> retMap = new HashMap<String, QueryBuilder>();
		if (query != null && attrQueryMap.size() > 0){
			for(Map.Entry<String, QueryBuilder> e : attrQueryMap.entrySet()){
				BoolQueryBuilder bQuery = QueryBuilders.boolQuery();
				bQuery.must(query);
				for(String key : attrQueryMap.keySet()){
					if (e.getKey().equals(key))
						continue;
					bQuery.must(attrQueryMap.get(key));
				}
				retMap.put(e.getKey(), bQuery);
				
			}
		}
		return retMap;
	}

	@Override
	public String getFl() {
		return this.fl;
	}

	@Override
	public void setFl(String fl) {
		this.fl = fl;
	}

	@Override
	public String getFq() { 
		return fq;
	}

	@Override
	public void setFq(String fq) {
		this.fq = fq;
	}

	@Override
	public Map<String, String> getFacetExt() {
		Map<String, String> ext = new HashMap<String, String>();
		if(this.facet_ext.length()>0){ 
			for(String str:this.facet_ext.split(",")){
				String[] tmp = str.split(":");
				ext.put(tmp[0], tmp[1]);
			}
		} 
		return ext;
	}
	
	@Override
	public void setRequestHandler(String handler) {
		this.requesthandler = handler; 
	}

	@Override
	public String getRequestHandler() { 
		return this.requesthandler;
	} 
	
	/**
	 * get Aggregation
	 * @param type  true is main , false is sub
	 * @param name
	 * @param field
	 * @param fun
	 * @return
	 */
	private AggregationBuilder genAgg(String fun,String name,String field,boolean type) {
		HashMap<String, String> kv = new HashMap<>();
		if(field.contains("(DEF(")) {
			kv = getDEF(field);
		}
		switch (fun) {
			case "cardinality": 
				return AggregationBuilders.cardinality(name).field(field); 
			case "avg":
				return AggregationBuilders.avg(name).field(field); 
			case "sum":
				if(field.contains("(script(")) {
					return AggregationBuilders.sum(name).script(getScript(field)); 
				}else {
					return AggregationBuilders.sum(name).field(field); 
				} 
			case "topHits":
				if(kv.size()>0) {
					TopHitsAggregationBuilder tb = AggregationBuilders.topHits(fun);
					if(kv.containsKey("sort")) {
						String sortField;
						SortOrder sod;
						if(kv.get("sort").endsWith(GlobalParam.SORT_ASC)) {
							sortField = kv.get("sort").substring(0, kv.get("sort").indexOf(GlobalParam.SORT_ASC));
							sod = SortOrder.ASC;
						}else {
							sortField = kv.get("sort").substring(0, kv.get("sort").indexOf(GlobalParam.SORT_DESC));
							sod = SortOrder.DESC;
						}
						tb.sort(sortField, sod).from(Integer.valueOf(name));
					} 
					if(kv.containsKey("size")) {
						tb.size(Integer.valueOf(kv.get("size")));
					}
					if(kv.containsKey("includes")) {
						tb.fetchSource(kv.get("includes").split("\\|"), null);
					}
					return tb.from(Integer.valueOf(name));
				}else {
					return AggregationBuilders.topHits(fun).from(Integer.valueOf(name)).size(Integer.valueOf(field));
				} 
		} 
		if(type) {
			Map<String, String> ext = getFacetExt();
			TermsAggregationBuilder tb = AggregationBuilders.terms(name).field(field);
			if(ext.containsKey("size")) {
				tb.size(Integer.valueOf(ext.get("size")));
			} 
			if(ext.containsKey("order")) {
				String[] tmp = ext.get("order").split(" ");
				if(tmp.length==2)
					tb.order(BucketOrder.aggregation(tmp[0], tmp[1].equals("desc")?false:true));
			}
			return tb;
		}
		return AggregationBuilders.terms(name).field(field);
	}
	
 private org.elasticsearch.script.Script getScript(String str) {
	 str = str.replace("(script(", "");
	 str = str.substring(0, str.length()-2); 
	 Script script = new Script(str); 
	 return script;
 }
 
 private HashMap<String, String> getDEF(String str){
	 str = str.replace("(DEF(", "");
	 str = str.substring(0, str.length()-2); 
	 HashMap<String, String> res = new HashMap<>();
	 for(String s:str.split(";")) {
		 String[] tmp = s.split("=");
		 if(tmp.length==2)
			 res.put(tmp[0], tmp[1]);
	 }
	 return res;
 }

}
