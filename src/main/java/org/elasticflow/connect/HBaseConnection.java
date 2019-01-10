package org.elasticflow.connect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
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
public class HBaseConnection extends FnConnectionSocket<Table>{
	private final static Logger log = LoggerFactory
			.getLogger("HBase Socket");
	private Configuration hbaseConfig;
	private Connection Hconn;
	private Table conn;

	public static FnConnectionSocket<?> getInstance(ConnectParams connectParams) {
		FnConnectionSocket<?> o = new HBaseConnection();
		o.init(connectParams);
		o.connect();
		return o;
	}

	@Override
	public void init(ConnectParams connectParams) {
		this.connectParams = connectParams;
		this.hbaseConfig = HBaseConfiguration.create();
		String ipString = ((WarehouseNosqlParam) connectParams.getWhp()).getPath();
		if (ipString != null && ipString.length() > 0) {
			String[] ips = ipString.split(",");
			StringBuilder ipStr = new StringBuilder();
			String port = null;
			if (ips.length > 0) {
				for (String ip : ips) {
					String[] ipPort = ip.split(":");
					if (ipPort != null && ipPort.length > 0) {
						if (ipStr.length() > 0)
							ipStr.append(",");
						ipStr.append(ipPort[0]);
					}
					if (ipPort != null && ipPort.length > 1)
						port = ipPort[1];
				}
			}
			if (ipStr.length() > 0)
				this.hbaseConfig.set("hbase.zookeeper.quorum", ipStr.toString());
			if (port != null && port.length() > 0){
				this.hbaseConfig.set("hbase.zookeeper.property.clientPort",	port);
			}else{
				this.hbaseConfig.set("hbase.zookeeper.property.clientPort",	"2181");
			} 
			this.hbaseConfig.set("hbase.client.write.buffer", "5242880");
		}
	}

	@Override
	public Table getConnection(boolean searcher) {
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
		try {
			if (this.conn != null ) {
				return true;
			}
		} catch (Exception e) {
			log.error("get status Exception,", e);
		}
		return false;
	}

	@Override
	public boolean free() {
		try {
			this.conn.close();
			this.Hconn.close();
			this.conn = null;
			this.connectParams = null;
		} catch (Exception e) {
			log.error("free connect Exception,", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean connect() {
		if (!status()) {
			try {
				this.Hconn = ConnectionFactory.createConnection(this.hbaseConfig);
				String tableColumnFamily = ((WarehouseNosqlParam) connectParams.getWhp()).getDefaultValue();
				if (tableColumnFamily != null && tableColumnFamily.length() > 0) {
					String[] strs = tableColumnFamily.split(":"); 
					this.conn = this.Hconn.getTable(
							TableName.valueOf(strs[0])); 
				} 
			} catch (Exception e) {
				log.error("HBase connect Exception,", e);
				return false;
			}
		}
		return true;
	}
}
