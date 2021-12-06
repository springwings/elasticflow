package org.elasticflow.connection;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2021-06-24 09:25
 */

public class HdfsConnection extends EFConnectionSocket<FileSystem> {
	
	final String DEFAULT_KEY = "username";
	
	private FileSystem conn = null;
	
	private final static Logger log = LoggerFactory.getLogger("Hdfs Socket");
	
	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new HdfsConnection();
		o.init(ConnectParams);
		return o;
	}
	
	@Override
	protected boolean connect(END_TYPE endType) {
		WarehouseParam wnp = this.connectParams.getWhp();
		if (wnp.getHost() != null) {
			if (!status()) { 			        	
				Configuration conf = new Configuration();
				conf.set("fs.defaultFS", wnp.getHost());
		        try {
					this.conn = FileSystem.get(new URI(wnp.getHost()), conf, 
							wnp.getDefaultValue().getString(DEFAULT_KEY));
				} catch (Exception e) {
					log.error("Hdfs connect Exception",e);
					this.conn = null;
				}
			}
		} else {
			return false;
		}
		return true;
	}

	@Override
	public FileSystem getConnection(END_TYPE endtype) {
		int tryTime = 0;
		try {
			while (tryTime < 5 && !connect(endtype)) {
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
		if(this.conn==null)
			return false;
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
			log.error("free connect Exception,", e);
			return false;
		}
		return true;
	}

}
