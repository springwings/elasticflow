package org.elasticflow.searcher.parser;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.MECHANISM;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Convert query to vearch search statement
 * @author chengwen
 * @version 5.x
 * @date 2022-10-26 09:23
 * @modify 2023-05-28 09:19
 */
public class VearchQueryParser implements QueryParser{
	
	JSONObject searchObj;
	
	public VearchQueryParser() {
		searchObj = new JSONObject();
	}
	
	public JSONObject getSearchObj() {
		return this.searchObj;
	}
 
	/**
	 * Parameter parsing as query parser 
	 * @param instanceConfig
	 * @param model 
	 */ 
	@Override
	public void parseQuery(InstanceConfig instanceConfig,SearcherModel<?> model) {
		Map<String, Object> paramMap = model.efRequest.getParams();
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
				model.storeId = String.valueOf(v);
			} else {
				if (instanceConfig.getSearchFields().containsKey(k)) { 
					if(instanceConfig.getSearchFields().get(k).getIndextype().toLowerCase().equals("vector")) {
						_feature.put("field",instanceConfig.getSearchFields().get(k).getAlias());
						_feature.put("feature",v); 
					}else {
						JSONObject filter = new JSONObject();
						filter.put(instanceConfig.getSearchFields().get(k).getAlias(),v);
						JSONObject row = new JSONObject();
						row.put("term", filter);
						filters.add(row);
					}
				} else {
					switch(k.toLowerCase()) {
					case "max_score":
						_feature.put("max_score",Float.parseFloat(String.valueOf(v))); 
						break; 
					case "min_score":
						_feature.put("min_score",Float.parseFloat(String.valueOf(v))); 
						break; 
					case "boost":
						_feature.put("boost",Float.parseFloat(String.valueOf(v))); 
						break; 
					} 
				}
			}
		}
		if(model.storeId==null && instanceConfig.getPipeParams().getWriteMechanism().equals(MECHANISM.Time)) {
			model.storeId = String.valueOf(Common.getNowZero());
		}
		if(_feature.size()==0) {
			Iterator<Map.Entry<String,EFField>> iter_tmp =  instanceConfig.getSearchFields().entrySet().iterator();
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
		searchObj.put("query", query);		
	}
 
	/**
	 * Parameter parsing as filter parser  
	 * @param instanceConfig
	 * @param model  
	 * @throws EFException
	 */
	@Override
	public void parseFilter(InstanceConfig instanceConfig,SearcherModel<?> model) throws EFException {
		 
	}
	 
}
