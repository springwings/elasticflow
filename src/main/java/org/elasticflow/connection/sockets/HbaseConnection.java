package org.elasticflow.connection.sockets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.connection.EFConnectionSocket;
import org.elasticflow.param.pipe.ConnectParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hbase basic connection establishment management class
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class HbaseConnection extends EFConnectionSocket<Table>{
	private final static Logger log = LoggerFactory
			.getLogger("HBase Socket");
	final String DEFAULT_KEY = "tableColumnFamily";
	private Configuration hbaseConfig;
	private Connection Hconn;

	public static EFConnectionSocket<?> getInstance(ConnectParams connectParams) {
		EFConnectionSocket<?> o = new HbaseConnection();
		o.init(connectParams);
		return o;
	}

	@Override
	public void init(ConnectParams connectParams) {
		this.connectParams = connectParams;
		this.hbaseConfig = HBaseConfiguration.create();
		String ipString = connectParams.getWhp().getHost();
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
	public boolean status() {
		try {
			if (this.conn != null ) {
				return true;
			}
		} catch (Exception e) {
			log.error("{} get hbase connection status exception,", this.connectParams.getWhp().getAlias(),e);
		}
		return false;
	}

	@Override
	public boolean free() {
		try {
			if(this.conn!=null)
				this.conn.close();
			if(this.Hconn!=null)
				this.Hconn.close();
			this.conn = null;
			this.connectParams = null;
		} catch (Exception e) {
			log.warn("{} free hbase connection exception", this.connectParams.getWhp().getAlias(),e);
			return false;
		}
		return true;
	}

	@Override
	public boolean connect(END_TYPE endType) {
		if (!status()) {
			try {
				this.Hconn = ConnectionFactory.createConnection(this.hbaseConfig);
				String tableColumnFamily = connectParams.getWhp().
						getDefaultValue().getString(DEFAULT_KEY);
				if (tableColumnFamily != null && tableColumnFamily.length() > 0) {
					String[] strs = tableColumnFamily.split(":"); 
					this.conn = this.Hconn.getTable(
							TableName.valueOf(strs[0])); 
				} 
			} catch (Exception e) {
				log.warn("HBase connect Exception,", e);
				return false;
			}
		}
		return true;
	}
}
