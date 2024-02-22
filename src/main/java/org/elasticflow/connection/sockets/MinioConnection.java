package org.elasticflow.connection.sockets;

import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.connection.EFConnectionSocket;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.minio.MinioClient;

/**
 * Minio basic connection establishment management class
 * @author chengwen
 * @version 1.0
 * @date 2021-06-24 09:25
 */
public class MinioConnection extends EFConnectionSocket<Object> {

	private MinioClient conn = null; 

	private final static Logger log = LoggerFactory.getLogger("Minio Socket"); 
	 

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new MinioConnection();
		o.init(ConnectParams);
		return o;
	}

	@Override
	public boolean connect(END_TYPE endType) {
		WarehouseParam wnp = this.connectParams.getWhp();
		if (wnp.getHost() != null) {
			if (!status()) { 			
				this.conn = MinioClient.builder()
	                    .endpoint(wnp.getHost())
	                    .credentials(wnp.getUser(), wnp.getPassword())
	                    .build();
			}
		} else {
			return false;
		}
		return true;
	} 

	@Override
	public Object getConnection(END_TYPE endType) {
		int tryTime = 0;
		try {
			while (tryTime < 5 && !connect(endType)) {
				tryTime++;
				Thread.sleep(1000+tryTime*500);
			}
		} catch (Exception e) {
			log.error("{} get kafka {} connection exception",this.connectParams.getWhp().getAlias(),endType.name(), e);
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
			if(this.conn!=null) { 
				this.conn = null;
			} 
			this.connectParams = null;
		} catch (Exception e) {
			log.warn("{} free minio connection exception", this.connectParams.getWhp().getAlias(),e);
			return false;
		}
		return true;
	}

}
