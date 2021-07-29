package org.elasticflow.model.searcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFSearchRequest;
import org.elasticflow.searcher.parser.SolrQueryParser;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class SearcherSolrModel implements SearcherModel<SolrQuery, String, String> {
	private SolrQuery query ; 
	private int start = 0;
	private int count = 5; 
	private boolean showQueryInfo = false;
	private String fq = "";
	private String fl="";
	private boolean cached=false;
	private String requesthandler="select";
	
	Map<String, List<String[]>> facetSearchParams;
	List<String> facetsConfig = new ArrayList<String>(); 
	private List<String> sortinfo;
	
	public static SearcherSolrModel getInstance(EFSearchRequest request, InstanceConfig instanceConfig) {
		SearcherSolrModel sq = new SearcherSolrModel(); 
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
	public int getStart() {
		return this.start;
	}

	@Override
	public void setStart(int start) {
		this.start = start;
	}

	@Override
	public int getCount() {
		return this.count;
	}

	@Override
	public void setCount(int count) {
		this.count = count;
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
	public boolean isShowQueryInfo() {
		return this.showQueryInfo;
	}

	@Override
	public void setShowQueryInfo(boolean isshow) {
		this.showQueryInfo = isshow;
	}
 
	@Override
	public List<String> getFacetsConfig() {
		// TODO Auto-generated method stub
		return null;
	} 

	@Override
	public boolean cacheRequest() { 
		return this.cached;
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
		return null;
	}

	@Override
	public void setRequestHandler(String handler) {
		this.requesthandler = handler; 
	}

	@Override
	public String getRequestHandler() { 
		return this.requesthandler;
	} 
}
