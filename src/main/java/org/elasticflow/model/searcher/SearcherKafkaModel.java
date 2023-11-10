package org.elasticflow.model.searcher;

import java.util.List;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFRequest;


/**
 * @description
 * @author chengwen
 * @version 5.0
 * @date 2023-02-22 09:08
 */
public class SearcherKafkaModel extends SearcherModel<String> { 
	 
	public static SearcherKafkaModel getInstance(EFRequest request, InstanceConfig instanceConfig) {
		SearcherKafkaModel SM = new SearcherKafkaModel(); 
		SM.setRequestHandler("");  
		SM.setEfRequest(request);
		return SM;
	} 

	@Override
	public List<String> getSortinfo() {
		return null;
	} 
  
}
