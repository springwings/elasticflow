package org.elasticflow.searcher.parser;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.util.EFException;

/**
 * Parse the query into the corresponding operating language command
 * @author chengwen
 * @version 1.0
 * @date 2018-01-23 09:19
 * @modify 2023-05-28 09:19
 */
public interface QueryParser {
	 
	public void parseQuery(InstanceConfig instanceConfig,SearcherModel<?> searcherModel);
	
	public void parseFilter(InstanceConfig instanceConfig,SearcherModel<?> searcherModel) throws EFException;
	
}
