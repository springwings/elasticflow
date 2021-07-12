package org.elasticflow.model.searcher;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public interface SearcherModel<T1, T2, T3> {
	public T1 getQuery();

	public void setQuery(T1 query);

	public void setStart(int param);

	public int getStart();

	public void setCount(int param);

	public int getCount();
	
	public String getFl();
	
	public void setRequestHandler(String handler);
	
	public String getRequestHandler();
	
	public void setFl(String fl);
	
	public String getFq();
	
	public void setFq(String fq); 
	
	public Map<String, String> getFacetExt();

	public Map<String,List<String[]>> getFacetSearchParams();
  
	public void setShowQueryInfo(boolean isshow);
	
	public boolean isShowQueryInfo();
	
	public List<T2> getSortinfo();  

	public List<T3> getFacetsConfig(); 

	public boolean cacheRequest();
}
