package org.elasticflow.model.searcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFRequest;
import org.elasticflow.util.instance.SearchParamUtil;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * @description
 * @author chengwen
 * @version 5.0
 * @date 2023-02-22 09:08
 */
public class SearcherESModel extends SearcherModel<SortBuilder<?>, AggregationBuilder> {
	 
	private List<SortBuilder<?>> sortinfo;
	private Map<String, List<String[]>> facetSearchParams;
	private List<AggregationBuilder> facetsConfig = new ArrayList<AggregationBuilder>();

	private boolean needCorpfuncCnt = false;
	private boolean cacheRequest = true;
	private Set<Integer> excludeSet;
	private String type;
	 
	public static SearcherESModel getInstance(EFRequest request, InstanceConfig instanceConfig) {
		SearcherESModel SM = new SearcherESModel(); 
		SM.setRequestHandler("");
		SM.setSorts(SearchParamUtil.getSortField(request, instanceConfig));
		SM.setFacetSearchParams(SearchParamUtil.getFacetParams(request, instanceConfig));
		if (request.getParam("facet_ext") != null) {
			SM.setFacet_ext((String) request.getParams().get("facet_ext"));
		} 
		SM.setEfRequest(request);
		return SM;
	}
 

	@Override
	public List<SortBuilder<?>> getSortinfo() {
		return sortinfo;
	}

	public void setSorts(List<SortBuilder<?>> sortinfo) {
		this.sortinfo = sortinfo;
	}

	@Override
	public Map<String, List<String[]>> getFacetSearchParams() {
		return facetSearchParams;
	}

	public void setFacetSearchParams(Map<String, List<String[]>> facetSearchParams) {
		this.facetSearchParams = facetSearchParams;
	}

	@Override
	public List<AggregationBuilder> getFacetsConfig() {
		if (facetSearchParams != null) {
			for (Map.Entry<String, List<String[]>> e : facetSearchParams.entrySet()) {
				int i = 0;
				AggregationBuilder agg = null;
				for (String[] strs : e.getValue()) {
					if (i == 0) {
						agg = genAgg(strs[0], strs[1], strs[2], true);
					} else {
						((AggregationBuilder) agg).subAggregation(genAgg(strs[0], strs[1], strs[2], false));
					}
					i++;
				}
				facetsConfig.add(agg);
			}
		}
		return facetsConfig;
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

	/**
	 * get Aggregation
	 * 
	 * @param type  true is main , false is sub
	 * @param name
	 * @param field
	 * @param fun
	 * @return
	 */
	private AggregationBuilder genAgg(String fun, String name, String field, boolean type) {
		HashMap<String, String> kv = new HashMap<>();
		if (field.contains("(DEF(")) {
			kv = getDEF(field);
		}
		switch (fun) {
		case "cardinality":
			return AggregationBuilders.cardinality(name).field(field);
		case "avg":
			return AggregationBuilders.avg(name).field(field);
		case "sum":
			if (field.contains("(script(")) {
				return AggregationBuilders.sum(name).script(getScript(field));
			} else {
				return AggregationBuilders.sum(name).field(field);
			}
		case "topHits":
			if (kv.size() > 0) {
				TopHitsAggregationBuilder tb = AggregationBuilders.topHits(fun);
				if (kv.containsKey("sort")) {
					String sortField;
					SortOrder sod;
					if (kv.get("sort").endsWith(GlobalParam.SORT_ASC)) {
						sortField = kv.get("sort").substring(0, kv.get("sort").indexOf(GlobalParam.SORT_ASC));
						sod = SortOrder.ASC;
					} else {
						sortField = kv.get("sort").substring(0, kv.get("sort").indexOf(GlobalParam.SORT_DESC));
						sod = SortOrder.DESC;
					}
					tb.sort(sortField, sod).from(Integer.valueOf(name));
				}
				if (kv.containsKey("size")) {
					tb.size(Integer.valueOf(kv.get("size")));
				}
				if (kv.containsKey("includes")) {
					tb.fetchSource(kv.get("includes").split("\\|"), null);
				}
				return tb.from(Integer.valueOf(name));
			} else {
				return AggregationBuilders.topHits(fun).from(Integer.valueOf(name)).size(Integer.valueOf(field));
			}
		}
		if (type) {
			Map<String, String> ext = getFacetExt();
			TermsAggregationBuilder tb = AggregationBuilders.terms(name).field(field);
			if (ext.containsKey("size")) {
				tb.size(Integer.valueOf(ext.get("size")));
			}
			if (ext.containsKey("order")) {
				String[] tmp = ext.get("order").split(" ");
				if (tmp.length == 2)
					tb.order(BucketOrder.aggregation(tmp[0], tmp[1].equals("desc") ? false : true));
			}
			return tb;
		}
		return AggregationBuilders.terms(name).field(field);
	}

	private org.elasticsearch.script.Script getScript(String str) {
		str = str.replace("(script(", "");
		str = str.substring(0, str.length() - 2);
		Script script = new Script(str);
		return script;
	}

	private HashMap<String, String> getDEF(String str) {
		str = str.replace("(DEF(", "");
		str = str.substring(0, str.length() - 2);
		HashMap<String, String> res = new HashMap<>();
		for (String s : str.split(";")) {
			String[] tmp = s.split("=");
			if (tmp.length == 2)
				res.put(tmp[0], tmp[1]);
		}
		return res;
	} 

}
