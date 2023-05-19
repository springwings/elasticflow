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
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:23
 */
public class VearchQueryParser implements QueryParser{
 
 
	/**
	 * Parameter parsing as query parser
	 * @param request
	 * @param instanceConfig
	 * @param model
	 * @param ssb
	 */
	static public void parseQuery(InstanceConfig instanceConfig,SearcherModel<?, ?> model,JSONObject ssb) {
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
					switch(k) {
					case "max_score":
						_feature.put("max_score",Float.parseFloat(String.valueOf(v))); 
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
		ssb.put("query", query);		
	}
 
	/**
	 * Parameter parsing as filter parser 
	 * @param request
	 * @param instanceConfig
	 * @param model
	 * @param ssb
	 * @throws EFException
	 */
	static public void parseFilter(InstanceConfig instanceConfig,SearcherModel<?, ?> model,SearchSourceBuilder ssb) throws EFException {
		 
	}
	 
}
