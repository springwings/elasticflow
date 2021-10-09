package org.elasticflow.connection;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseNosqlParam;
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
	private CloudSolrClient conn = null;

	private final static Logger log = LoggerFactory.getLogger("Solr Socket");

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new SolrConnection();
		o.init(ConnectParams);
		o.connect();
		return o;
	}

	@Override
	public boolean connect() {
		WarehouseNosqlParam wnp = (WarehouseNosqlParam) this.connectParams.getWhp();
		if (wnp.getPath() != null) {
			if (!status()) {
				this.conn = new CloudSolrClient(wnp.getPath());
				this.conn.setZkClientTimeout(zkClientTimeout);
				this.conn.setZkConnectTimeout(zkConnectTimeout);
			}
		} else {
			return false;
		}
		return true;
	}

	@Override
	public CloudSolrClient getConnection(END_TYPE endType) {
		int tryTime = 0;
		try {
			while (tryTime < 5 && !connect()) {
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
		if (this.conn == null) {
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
