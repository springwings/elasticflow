package org.elasticflow.connect;

import java.net.InetAddress;
import java.util.HashMap;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class ESConnection extends FnConnectionSocket implements FnConnection<ESConnector> {

	private Client conn;
	private BulkProcessor bulkProcessor;
	private ESConnector ESC = new ESConnector();

	private final static int BULK_BUFFER = 1000;
	private final static int BULK_SIZE = 30;
	private final static int BULK_FLUSH_SECONDS = 3;
	private final static int BULK_CONCURRENT = 1;

	private final static Logger log = LoggerFactory.getLogger(ESConnection.class);

	public static FnConnection<?> getInstance(HashMap<String, Object> ConnectParams) {
		FnConnection<?> o = new ESConnection();
		o.init(ConnectParams);
		o.connect();
		return o;
	}

	@Override
	public boolean connect() {
		if (this.connectParams.get("ip") != null) {
			if (!status()) {
				Settings settings = Settings.builder()
				        .put("client.transport.sniff", true)
				        .put("cluster.name", String.valueOf(this.connectParams.get("name"))).build(); 
				this.conn = new PreBuiltTransportClient(settings);
				String Ips = (String) this.connectParams.get("ip");
				for (String ip : Ips.split(",")) {
					try {
						((TransportClient) this.conn)
								.addTransportAddress(new TransportAddress(InetAddress.getByName(ip), 9300));
					} catch (Exception e) {
						log.error("connect Exception", e);
					}
				}
				this.ESC.setClient(this.conn);
			} 
		} else {
			return false;
		}
		return true;
	}

	@Override
	public ESConnector getConnection(boolean searcher) {
		connect();
		if (!searcher) {
			if (this.bulkProcessor == null) {
				getBulkProcessor(this.conn);
				this.ESC.setBulkProcessor(this.bulkProcessor);
			} 
		}
		this.ESC.setRunState(true);
		return this.ESC;
	}

	@Override
	public boolean status() {
		if (this.conn == null || this.conn.admin() == null) {
			return false;
		}
		return true;
	}

	@Override
	public boolean free() {
		try {
			freeBP();
			this.conn.close();  
			this.ESC = null;
			this.conn = null;
			this.connectParams = null;
		} catch (Exception e) {
			log.error("free connect Exception,", e);
			return false;
		}
		return true;
	}
	
	private void freeBP() {
		if (this.bulkProcessor != null) {
			this.bulkProcessor.flush();
			this.bulkProcessor.close();
			this.bulkProcessor = null;
		}
	}

	private void getBulkProcessor(Client _client) {
		this.bulkProcessor = BulkProcessor.builder(_client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				if (response.hasFailures()) {
					log.error("BulkProcessor error," + response.buildFailureMessage());
					ESC.setRunState(false);
				}else {
					ESC.setRunState(true);
				}
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				if (failure != null) {
					failure.printStackTrace();
				}
			}
		}).setBulkActions(BULK_BUFFER).setBulkSize(new ByteSizeValue(BULK_SIZE, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(BULK_FLUSH_SECONDS))
				.setConcurrentRequests(BULK_CONCURRENT).build(); 
	}
}
