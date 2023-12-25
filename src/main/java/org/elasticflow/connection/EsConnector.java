package org.elasticflow.connection;

import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * ElasticSearch Advanced Packaging Connection
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public final class EsConnector {
	
	private volatile RestHighLevelClient client;
	private volatile BulkProcessor bulkProcessor;
	private AtomicBoolean bulkRunState  = new AtomicBoolean(true);
	private String infos="";
	private String alias = "";
	
	public RestHighLevelClient getClient() {
		return client;
	}
	public void setClient(RestHighLevelClient client,String alias) {
		this.client = client;
		this.alias = alias;
	}
	
	public String getAlias() {
		return this.alias;
	}
	
	public boolean getRunState() {
		return bulkRunState.get();
	}
	public void setRunState(boolean state) {
		bulkRunState.set(state);
	}
	public BulkProcessor getBulkProcessor() {
		return bulkProcessor;
	}
	public void setBulkProcessor(BulkProcessor bulkProcessor) {
		this.bulkProcessor = bulkProcessor;
	}
	
	public void setInfos(String infos) {
		this.infos = infos;
	}
	
	public String getInfos() {
		String tmp = this.infos;
		this.infos = "";
		return tmp;
	}
	
}
