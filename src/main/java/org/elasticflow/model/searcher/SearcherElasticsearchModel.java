package org.elasticflow.model.searcher;

import java.util.List;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFRequest;
import org.elasticflow.util.instance.SearchParamUtil;
import org.elasticsearch.search.sort.SortBuilder;

/**
 * @description
 * @author chengwen
 * @version 5.0
 * @date 2023-02-22 09:08
 */
public class SearcherElasticsearchModel extends SearcherModel<SortBuilder<?>> {
	 
	private List<SortBuilder<?>> sortinfo;  
	 
	public static SearcherElasticsearchModel getInstance(EFRequest request, InstanceConfig instanceConfig) {
		SearcherElasticsearchModel SM = new SearcherElasticsearchModel(); 
		SM.setRequestHandler("");
		SM.setSorts(SearchParamUtil.getSortField(request, instanceConfig));
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
}
