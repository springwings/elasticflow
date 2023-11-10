package org.elasticflow.model.searcher;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.EFRequest;


/**
 * @description
 * @author chengwen
 * @version 5.0
 * @date 2023-02-22 09:08
 */
public class SearcherVearchModel extends SearcherModel<String> {
	 
	public static SearcherVearchModel getInstance(EFRequest request, InstanceConfig instanceConfig) {
		SearcherVearchModel sq = new SearcherVearchModel();
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
	public List<String> getSortinfo() {
		// TODO Auto-generated method stub
		return null;
	}
}
