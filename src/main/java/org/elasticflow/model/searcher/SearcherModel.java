package org.elasticflow.model.searcher;

import java.util.List;

import org.elasticflow.model.EFRequest;

import com.alibaba.fastjson.JSONObject;

/**
 * Search Request Description Model
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public abstract class SearcherModel<T1> {

	/**
	 * Resource identification
	 */
	public String storeId; 
	
	/**
	 * Fields returned by search results
	 */
	private String fl;
	
	/**
	 * Whether to return the instance (cluster) status
	 */
	private boolean showStats = false;
	
	/**
	 * Do you want to enable highlighting of results,check highlightfields
	 */  
	private String[] highlightfields = null;
	
	private String highLightTag = "em";
	
	/**
	 * User query parameters
	 */
	public EFRequest efRequest; 
	
	/**
	 * User query start
	 */
	private int start = 0;
	
	/**
	 * Number of data returned by user query
	 */
	private int count = 5;

	/**
	 * Whether to conduct a search and return content 
	 * for score and sentence interpretation
	 */
	private boolean showQueryInfo = false;
	
	/**
	 * search processor
	 */
	private String requesthandler;
	
	/**
	 * Aggregate search parameters
	 */
	private JSONObject customquery;

	public abstract List<T1> getSortinfo();
	
	public boolean isShowQueryInfo() {
		return this.showQueryInfo;
	}

	public void setShowQueryInfo(boolean isshow) {
		this.showQueryInfo = isshow;
	}

	public void setRequestHandler(String handler) {
		this.requesthandler = handler;
	}
	
	public void setEfRequest(EFRequest efRequest) {
		this.efRequest = efRequest;
	}

	public String getRequestHandler() {
		return this.requesthandler;
	}

	/**
	 * filter fields
	 * @return
	 */
	public String getFl() {
		return this.fl;
	};

	public String getStoreId() {
		return this.storeId;
	}

	public void setFl(Object fl) {
		if(fl!=null)
			this.fl = String.valueOf(fl);
	}
	  
	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	} 
	
	public String[] getHighlightFields() {
		return highlightfields;
	}

	public void setHighlightFields(Object highlightFields) {
		if(highlightFields!=null)
			this.highlightfields = String.valueOf(highlightFields).split(",");
	}
	
	public String getHighlightTag() {
		return highLightTag;
	}

	public void setHighlightTag(Object highLightTag) {
		if(highLightTag!=null)
			this.highLightTag = String.valueOf(highLightTag);
	}
	
	public boolean isShowStats() {
		return showStats;
	}

	public void setShowStats(boolean showStats) {
		this.showStats = showStats;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	} 

	public void setCustomquery(String customquery) {
		this.customquery = JSONObject.parseObject(customquery);
	}

	public JSONObject getCustomQuery() { 
		return this.customquery;
	}
}
