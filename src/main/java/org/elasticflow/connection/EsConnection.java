package org.elasticflow.connection;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class EsConnection extends EFConnectionSocket<EsConnector> {

	private RestHighLevelClient conn;
	private BulkProcessor bulkProcessor;
	private EsConnector ESC = new EsConnector();

	private final static int BULK_BUFFER = 1000;
	private final static int BULK_SIZE = 30;
	private final static int BULK_FLUSH_SECONDS = 3;
	private final static int BULK_CONCURRENT = 1;

	private CredentialsProvider credentialsProvider;

	private final static Logger log = LoggerFactory.getLogger(EsConnection.class);

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new EsConnection();
		o.init(ConnectParams);
		return o;
	}

	@Override
	protected boolean connect(END_TYPE endType) {
		WarehouseParam wnp = this.connectParams.getWhp();
		if (wnp.getHost() != null) {
			if (wnp.getPassword() != null) {
				credentialsProvider = new BasicCredentialsProvider();
				credentialsProvider.setCredentials(AuthScope.ANY,
						new UsernamePasswordCredentials(wnp.getUser(), wnp.getPassword()));
			}
			if (!status()) {
				String[] hosts = wnp.getHost().split(",");
				HttpHost[] httpHosts = new HttpHost[hosts.length];
				for (int i = 0; i < hosts.length; i++) {
					try {
						httpHosts[i] = new HttpHost(hosts[i], 9200, "http");
					} catch (Exception e) {
						log.error("connect Exception", e);
					}
				}
				if(credentialsProvider!=null) {
					this.conn = new RestHighLevelClient(RestClient.builder(httpHosts)
							.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
								public HttpAsyncClientBuilder customizeHttpClient(
										HttpAsyncClientBuilder httpClientBuilder) {
									httpClientBuilder.disableAuthCaching();
									return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
								}
							}));
				}else {
					this.conn = new RestHighLevelClient(RestClient.builder(httpHosts));
				}
				this.ESC.setClient(this.conn);
			}
		} else {
			return false;
		}
		return true;
	}

	@Override
	public EsConnector getConnection(END_TYPE endType) {
		connect(endType);
		if (endType != END_TYPE.searcher) {
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
		if (this.conn == null) {
			return false;
		}
		return true;
	}

	@Override
	public boolean free() {
		try {
			if (this.conn != null)
				this.conn.close();
			this.ESC = null;
			this.conn = null;
			this.connectParams = null;
		} catch (Exception e) {
			log.warn("free connect Exception,", e);
			return false;
		}
		return true;
	}

	@SuppressWarnings("deprecation")
	private void getBulkProcessor(RestHighLevelClient _client) {
		this.bulkProcessor = BulkProcessor
				.builder((request, bulkListener) -> _client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
						new BulkProcessor.Listener() {
							@Override
							public void beforeBulk(long executionId, BulkRequest request) {

							}

							@Override
							public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
								if (response.hasFailures()) {
									ESC.setRunState(false);
									ESC.setInfos(response.buildFailureMessage());
								}
							}

							@Override
							public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
								if (failure != null) {
									ESC.setRunState(false);
									ESC.setInfos(failure.getMessage());
								}
							}
						})
				.setBulkActions(BULK_BUFFER).setBulkSize(new ByteSizeValue(BULK_SIZE, ByteSizeUnit.MB))
				.setFlushInterval(TimeValue.timeValueSeconds(BULK_FLUSH_SECONDS)).setConcurrentRequests(BULK_CONCURRENT)
				.build();

	}
}
