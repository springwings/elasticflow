package org.elasticflow.model.searcher;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFSearchRequest;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class SearcherVearchModel extends SearcherModel<String, String, String> {

	public static SearcherVearchModel getInstance(EFSearchRequest request, InstanceConfig instanceConfig) {
		SearcherVearchModel sq = new SearcherVearchModel();
		parse(request, sq, instanceConfig);
		return sq;
	}

	public static void parse(EFSearchRequest request, SearcherVearchModel current, InstanceConfig instanceConfig) {
		Map<String, Object> paramMap = request.getParams();
		Set<Entry<String, Object>> entries = paramMap.entrySet();
		Iterator<Entry<String, Object>> iter = entries.iterator();
		JSONObject fq = new JSONObject();
		fq.put("query", new JSONObject());
		JSONArray _jarr = new JSONArray();
		JSONObject _jObject = new JSONObject();
		while (iter.hasNext()) {
			Entry<String, Object> entry = iter.next();
			String k = entry.getKey();
			String v = String.valueOf(entry.getValue());
			if (k.equals(GlobalParam.PARAM_STORE_ID)) {
				current.storeId = v;
			} else {
				if (instanceConfig.getWriteFields().containsKey(k)) { 
					if(instanceConfig.getWriteFields().get(k).getIndextype().toLowerCase().equals("vector")) {
						_jObject.put("field","features");
						_jObject.put("feature",v); 
					}  
				} else {
					if(k.equals("max_score")) {
						_jObject.put("max_score",Float.parseFloat(v)); 
					}
				}
			}
		}
		_jarr.add(_jObject);
		fq.getJSONObject("query").put("sum",_jarr);
		current.setFq(fq.toString());
	}

	@Override
	public String getQuery() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setQuery(String query) {
		// TODO Auto-generated method stub

	}
  
	@Override
	public Map<String, List<String[]>> getFacetSearchParams() {
		// TODO Auto-generated method stub
		return null;
	}
 
	@Override
	public List<String> getSortinfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getFacetsConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean cacheRequest() {
		// TODO Auto-generated method stub
		return false;
	}

}
