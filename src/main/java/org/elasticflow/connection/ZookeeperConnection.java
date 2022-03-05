package org.elasticflow.connection;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
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
	
	private final static Logger log = LoggerFactory
			.getLogger("Zookeeper Socket");

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new ZookeeperConnection();
		o.init(ConnectParams);
		return o;
	} 

	@Override
	protected boolean connect(END_TYPE endType) {
		if (!status()) {
			try { 
				this.conn = new ZooKeeper(
						this.connectParams.getWhp().getHost(),
						CONNECTION_TIMEOUT, (Watcher) this.connectParams.getPlugin()); 
			} catch (Exception e) {
				log.error("connection Exception", e);
				return false;
			}
		}
		return true;
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
