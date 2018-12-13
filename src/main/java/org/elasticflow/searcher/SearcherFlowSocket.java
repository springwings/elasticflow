package org.elasticflow.searcher;

import java.util.HashMap;

import org.apache.lucene.analysis.Analyzer;

import org.elasticflow.config.GlobalParam.DATA_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.flow.Flow;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.searcher.handler.Handler;
import org.elasticflow.util.FNException;

/** 
 * @author chengwen
 * @version 2.0 
 */
public abstract class SearcherFlowSocket extends Flow{
	
	protected Analyzer analyzer;
	protected InstanceConfig instanceConfig;  
	
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		this.poolName = String.valueOf(connectParams.get("poolName"));
		this.instanceConfig = (InstanceConfig) this.connectParams.get("instanceConfig");
		this.analyzer = (Analyzer) this.connectParams.get("analyzer");
	} 
	
	public Analyzer getAnalyzer() {
		return this.analyzer;
	}
	/**need rewrite*/
	public abstract SearcherResult Search(SearcherModel<?, ?, ?> query, String instance,Handler handler) throws FNException;
	
	public DATA_TYPE getType() {
		return (DATA_TYPE) connectParams.get("type");
	}   
 
}
