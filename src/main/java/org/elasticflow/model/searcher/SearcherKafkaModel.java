package org.elasticflow.model.searcher;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFRequest;
import org.elasticflow.util.instance.SearchParamUtil;


/**
 * @description
 * @author chengwen
 * @version 5.0
 * @date 2023-02-22 09:08
 */
public class SearcherKafkaModel extends SearcherModel<String, String> {
	  
	private Map<String, List<String[]>> facetSearchParams;  
	private boolean cacheRequest = true;
	private Set<Integer> excludeSet;
	private String type;
	 
	public static SearcherKafkaModel getInstance(EFRequest request, InstanceConfig instanceConfig) {
		SearcherKafkaModel SM = new SearcherKafkaModel(); 
		SM.setRequestHandler(""); 
		SM.setFacetSearchParams(SearchParamUtil.getFacetParams(request, instanceConfig));
		if (request.getParam("facet_ext") != null) {
			SM.setFacet_ext((String) request.getParams().get("facet_ext"));
		} 
		SM.setEfRequest(request);
		return SM;
	}
 

	@Override
	public List<String> getSortinfo() {
		return null;
	}
 
	@Override
	public Map<String, List<String[]>> getFacetSearchParams() {
		return facetSearchParams;
	}

	public void setFacetSearchParams(Map<String, List<String[]>> facetSearchParams) {
		this.facetSearchParams = facetSearchParams;
	}

	@Override
	public List<String> getFacetsConfig() {
		// TODO Auto-generated method stub
		return null;
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
  
}
