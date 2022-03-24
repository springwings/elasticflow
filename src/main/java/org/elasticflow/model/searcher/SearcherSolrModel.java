package org.elasticflow.model.searcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFRequest;
import org.elasticflow.searcher.parser.SolrQueryParser;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class SearcherSolrModel extends SearcherModel<SolrQuery, String, String> {
	
	private SolrQuery query ; 

	private boolean cached=false; 
	
	Map<String, List<String[]>> facetSearchParams;
	List<String> facetsConfig = new ArrayList<String>(); 
	private List<String> sortinfo;
	
	public static SearcherSolrModel getInstance(EFRequest request, InstanceConfig instanceConfig) {
		SearcherSolrModel sq = new SearcherSolrModel(); 
		sq.setRequestHandler("select");
		sq.setQuery(SolrQueryParser.parseRequest(request, instanceConfig));		
		return sq;
	}
 
	@Override
	public SolrQuery getQuery() {
		return this.query;
	}

	@Override
	public void setQuery(SolrQuery query) {
		this.query = query;
	}

	@Override
	public Map<String,List<String[]>> getFacetSearchParams() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getSortinfo() {
		return this.sortinfo;
	}

	public List<String> setSortinfo(List<String> sortinfo) {
		return this.sortinfo = sortinfo;
	} 

	@Override
	public boolean cacheRequest() { 
		return this.cached;
	}

	@Override
	public List<String> getFacetsConfig() {
		// TODO Auto-generated method stub
		return null;
	}   
}
