package org.elasticflow.connection;

import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public final class EsConnector {
	
	private volatile RestHighLevelClient client;
	private volatile BulkProcessor bulkProcessor;
	private AtomicBoolean bulkRunState  = new AtomicBoolean(true);
	
	public RestHighLevelClient getClient() {
		return client;
	}
	public void setClient(RestHighLevelClient client) {
		this.client = client;
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
	
}
