package org.elasticflow.model.searcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public abstract class SearcherModel<T1, T2, T3> {

	protected String storeId;

	private String fq;

	private String fl;

	private int start = 0;

	private int count = 5;

	private boolean showQueryInfo = false;

	private String requesthandler;

	private String facet_ext = "";

	public abstract T1 getQuery();

	public abstract void setQuery(T1 query);

	public abstract Map<String, List<String[]>> getFacetSearchParams();

	public abstract List<T2> getSortinfo();

	public abstract boolean cacheRequest();
	
	public abstract List<T3> getFacetsConfig();

	public boolean isShowQueryInfo() {
		return this.showQueryInfo;
	}

	public void setShowQueryInfo(boolean isshow) {
		this.showQueryInfo = isshow;
	}

	public void setRequestHandler(String handler) {
		this.requesthandler = handler;
	}

	public String getRequestHandler() {
		return this.requesthandler;
	}

	public String getFl() {
		return this.fl;
	};

	public String getStoreId() {
		return this.storeId;
	}

	public void setFl(String fl) {
		this.fl = fl;
	}

	public String getFq() {
		return this.fq;
	}

	public void setFq(String fq) {
		this.fq = fq;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
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
