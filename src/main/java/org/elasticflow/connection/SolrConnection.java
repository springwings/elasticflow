package org.elasticflow.connection;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class SolrConnection extends EFConnectionSocket<CloudSolrClient> {

	private final static int zkClientTimeout = 180000;
	private final static int zkConnectTimeout = 60000; 

	private final static Logger log = LoggerFactory.getLogger("Solr Socket");

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new SolrConnection();
		o.init(ConnectParams);
		return o;
	}

	@Override
	protected boolean connect(END_TYPE endType) {
		WarehouseParam wnp = this.connectParams.getWhp();
		if (wnp.getHost() != null) {
			if (!status()) {
				this.conn = new CloudSolrClient(wnp.getHost());
				this.conn.setZkClientTimeout(zkClientTimeout);
				this.conn.setZkConnectTimeout(zkConnectTimeout);
			}
		} else {
			return false;
		}
		return true;
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
			if(this.conn!=null)
				this.conn.close();
			this.conn = null;
			this.connectParams = null;
		} catch (Exception e) {
			log.warn("free connect Exception,", e);
			return false;
		}
		return true;
	}

}
