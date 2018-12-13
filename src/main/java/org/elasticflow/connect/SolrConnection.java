package org.elasticflow.connect;

import java.util.HashMap;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class SolrConnection extends FnConnectionSocket implements FnConnection<CloudSolrClient> {

	private final static int zkClientTimeout = 180000;
	private final static int zkConnectTimeout = 60000;  
	private CloudSolrClient conn = null; 

	private final static Logger log = LoggerFactory
			.getLogger("Solr Socket");

	public static FnConnection<?> getInstance(
			HashMap<String, Object> ConnectParams) {
		FnConnection<?> o = new SolrConnection();
		o.init(ConnectParams);
		o.connect();
		return o;
	} 

	@Override
	public boolean connect() {
		if (this.connectParams.get("ip") != null) {
			if (!status()) {
				this.conn = new CloudSolrClient(
						(String) this.connectParams.get("ip"));
				this.conn.setZkClientTimeout(zkClientTimeout);
				this.conn.setZkConnectTimeout(zkConnectTimeout);
			}
		} else {
			return false;
		}
		return true;
	}

	@Override
	public CloudSolrClient getConnection(boolean searcher) {
		int tryTime=0;
		try {
			while(tryTime<5 && !connect()){ 
				tryTime++;
				Thread.sleep(2000); 
			} 
		} catch (Exception e) {
			log.error("try to get Connection Exception,", e);
		}
		return this.conn;
	}

	@Override
	public boolean status() {
		if (this.conn == null ) {
			return false;
		} 
		return true;
	}

	@Override
	public boolean free() {
		try {
			this.conn.close();
			this.conn = null;
			this.connectParams = null;
		} catch (Exception e) {
			log.error("free connect Exception,", e);
			return false;
		}
		return true;
	} 

}
