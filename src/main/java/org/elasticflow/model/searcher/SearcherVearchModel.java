package org.elasticflow.model.searcher;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.EFRequest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class SearcherVearchModel extends SearcherModel<String, String, String> {

	public static SearcherVearchModel getInstance(EFRequest request, InstanceConfig instanceConfig) {
		SearcherVearchModel sq = new SearcherVearchModel();
		parse(request, sq, instanceConfig);
		return sq;
	}

	public static void parse(EFRequest request, SearcherVearchModel current, InstanceConfig instanceConfig) {
		Map<String, Object> paramMap = request.getParams();
		Set<Entry<String, Object>> entries = paramMap.entrySet();
		Iterator<Entry<String, Object>> iter = entries.iterator();
		JSONObject query = new JSONObject(); 
		JSONArray _jarr = new JSONArray();
		JSONArray filters = new JSONArray();
		JSONObject _feature = new JSONObject();
		while (iter.hasNext()) {
			Entry<String, Object> entry = iter.next();
			String k = entry.getKey();
			Object v = entry.getValue();
			if (k.equals(GlobalParam.KEY_PARAM.__storeid.name())) {
				current.storeId = String.valueOf(v);
			} else {
				if (instanceConfig.getWriteFields().containsKey(k)) { 
					if(instanceConfig.getWriteFields().get(k).getIndextype().toLowerCase().equals("vector")) {
						_feature.put("field",instanceConfig.getWriteFields().get(k).getAlias());
						_feature.put("feature",v); 
					}else {
						JSONObject filter = new JSONObject();
						filter.put(instanceConfig.getWriteFields().get(k).getAlias(),v);
						JSONObject row = new JSONObject();
						row.put("term", filter);
						filters.add(row);
					}
				} else {
					switch(k) {
					case "max_score":
						_feature.put("max_score",Float.parseFloat(String.valueOf(v))); 
						break; 
					} 
				}
			}
		}
		if(_feature.size()==0) {
			Iterator<Map.Entry<String,EFField>> iter_tmp =  instanceConfig.getWriteFields().entrySet().iterator();
			while (iter_tmp.hasNext()) {
				Entry<String, EFField> entry = iter_tmp.next();
				if(entry.getValue().getIndextype().toLowerCase().equals("vector")) {					
					_feature.put("field",entry.getValue().getAlias());
					int size = (JSON.parseObject(entry.getValue().getDsl())).getIntValue("dimension");
					float[] vec = new float[size];
					for (int i = 0; i < size; i++) {
						vec[i] = 0.F;
			        }
					_feature.put("feature",vec); 
					_feature.put("max_score",Float.MAX_VALUE); 
					break;
				}				
			}
		}
		_jarr.add(_feature);
		query.put("sum",_jarr);
		if(filters.size()>0)
			query.put("filter", filters);
		current.setFq(query.toJSONString());
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
