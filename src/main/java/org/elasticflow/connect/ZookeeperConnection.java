package org.elasticflow.connect;

import java.util.HashMap;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class ZookeeperConnection extends FnConnectionSocket implements FnConnection<ZooKeeper> {

	private final static int CONNECTION_TIMEOUT = 50000; 
	
	private ZooKeeper conn; 
	
	private final static Logger log = LoggerFactory
			.getLogger("Zookeeper Socket");

	public static FnConnection<?> getInstance(
			HashMap<String, Object> ConnectParams) {
		FnConnection<?> o = new ZookeeperConnection();
		o.init(ConnectParams);
		o.connect();
		return o;
	} 

	@Override
	public boolean connect() {
		if (!status()) {
			try { 
				this.conn = new ZooKeeper(
						(String) this.connectParams.get("ip"),
						CONNECTION_TIMEOUT, (Watcher) this.connectParams.get("watcher")); 
			} catch (Exception e) {
				log.error("connection Exception", e);
				return false;
			}
		}
		return true;
	}

	@Override
	public ZooKeeper getConnection(boolean searcher) {
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
