package org.elasticflow.model.searcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.KEY_PARAM;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.EFRequest;

import com.alibaba.fastjson.JSONObject;


/**
 * @description
 * @author chengwen
 * @version 5.0
 * @date 2023-02-22 09:08
 */
public class SearcherVearchModel extends SearcherModel<JSONObject> {
	
	private List<JSONObject> sortinfo;
	
	public static SearcherVearchModel getInstance(EFRequest request, InstanceConfig instanceConfig) {
		SearcherVearchModel sq = new SearcherVearchModel();
		sq.setSorts(getSortField(request, instanceConfig));
		sq.setEfRequest(request);
		return sq;
	} 
	
	public static boolean containField(Map<String, EFField> fields,String k) {
		for (Entry<String, EFField> entry : fields.entrySet()) {
			if(entry.getValue().getAlias().equals(k)) {
				return true;
			}
		}	
		return false;
	} 
  
	@Override
	public List<JSONObject> getSortinfo() {
		return this.sortinfo;
	}
	
	public void setSorts(List<JSONObject> sortinfo) {
		this.sortinfo = sortinfo;
	} 
	
	static List<JSONObject> getSortField(EFRequest request, InstanceConfig instanceConfig) {
		String sortstrs = (String) request.getParam(KEY_PARAM.sort.name());
		List<JSONObject> sortList = new ArrayList<JSONObject>(); 
		if (sortstrs != null && sortstrs.length() > 0) {
			boolean reverse = false;
			String[] sortArr = sortstrs.split(",");
			String fieldname = "";
			for (String str : sortArr) {
				str = str.trim();
				if (str.endsWith(GlobalParam.SORT_DESC)) {
					reverse = true;
					fieldname = str.substring(0, str.indexOf(GlobalParam.SORT_DESC));
				} else if (str.endsWith(GlobalParam.SORT_ASC)) {
					reverse = false;
					fieldname = str.substring(0, str.indexOf(GlobalParam.SORT_ASC));
				} else {
					reverse = false;
					fieldname = str;
				}
				JSONObject sort = new JSONObject();
				if(reverse) {
					sort.put(fieldname,new JSONObject().put("order", "desc"));
				}else {
					sort.put(fieldname,new JSONObject().put("order", "asc"));
				} 
				sortList.add(sort); 
			}
		} 
		return sortList;
	}
}
