package org.elasticflow.connection;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseNosqlParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2019-01-09 15:02
 */
public class ZookeeperConnection extends EFConnectionSocket<ZooKeeper> {

	private final static int CONNECTION_TIMEOUT = 50000; 
	
	private ZooKeeper conn; 
	
	private final static Logger log = LoggerFactory
			.getLogger("Zookeeper Socket");

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new ZookeeperConnection();
		o.init(ConnectParams);
		o.connect();
		return o;
	} 

	@Override
	public boolean connect() {
		if (!status()) {
			try { 
				this.conn = new ZooKeeper(
						((WarehouseNosqlParam) this.connectParams.getWhp()).getPath(),
						CONNECTION_TIMEOUT, (Watcher) this.connectParams.getPlugin()); 
			} catch (Exception e) {
				log.error("connection Exception", e);
				return false;
			}
		}
		return true;
	}

	@Override
	public ZooKeeper getConnection(END_TYPE endType) {
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
		if (this.conn == null || this.conn.getState().equals(States.CLOSED) ) {
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
