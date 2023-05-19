package org.elasticflow.model.searcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticflow.model.EFRequest;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public abstract class SearcherModel<T1, T2> {

	public String storeId; 
	 
	private String fl;
	
	private boolean showStats = false;
	
	public EFRequest efRequest; 

	private int start = 0;

	private int count = 5;

	private boolean showQueryInfo = false;

	private String requesthandler;

	private String facet_ext = ""; 

	public abstract Map<String, List<String[]>> getFacetSearchParams();

	public abstract List<T1> getSortinfo();

	public abstract boolean cacheRequest(); 
	
	public abstract List<T2> getFacetsConfig(); 
	
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

	public void setFl(String fl) {
		this.fl = fl;
	}
	  
	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
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

	public void setFacet_ext(String facet_ext) {
		this.facet_ext = facet_ext;
	}

	public Map<String, String> getFacetExt() {
		Map<String, String> ext = new HashMap<String, String>();
		if (this.facet_ext.length() > 0) {
			for (String str : this.facet_ext.split(",")) {
				String[] tmp = str.split(":");
				ext.put(tmp[0], tmp[1]);
			}
		}
		return ext;
	}
}
