package org.elasticflow.connect;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.DATA_TYPE;

/**
 * all source connection manager
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-21 11:02
 */
@ThreadSafe
public final class FnConnectionPool {

	private final static FnConnectionPool FnCPool;

	private ConcurrentHashMap<String, ConnectionPool> pools = new ConcurrentHashMap<>();

	private int waitTime = 1000;

	private final static Logger log = LoggerFactory.getLogger("FnConnectionPool");

	static {
		FnCPool = new FnConnectionPool();
	}

	/**
	 * @param canShareConn
	 *            judge is support share connection to deal
	 * @return
	 */
	public static FnConnection<?> getConn(HashMap<String, Object> params, String poolName, boolean canShareConn) {
		return FnCPool.getConnection(params, poolName, canShareConn);
	}

	public static void freeConn(FnConnection<?> conn, String poolName, boolean releaseConn) {
		FnCPool.freeConnection(poolName, conn, releaseConn);
	}

	public static String getStatus(String poolName) {
		return FnCPool.getState(poolName);
	}

	public static void release(String poolName) {
		FnCPool.releasePool(poolName);
	}

	/**
	 * release pools
	 */
	private void releasePool(String poolName) {
		synchronized (this.pools) {
			if (poolName != null) {
				if (this.pools.containsKey(poolName))
					this.pools.get(poolName).releaseAll();
			} else {
				for (Entry<String, ConnectionPool> ent : this.pools.entrySet()) {
					ent.getValue().releaseAll();
				}
			}
		}
	}

	/**
	 * get connection from pool and waiting
	 */
	private FnConnection<?> getConnection(HashMap<String, Object> params, String poolName, boolean canShareConn) {
		synchronized (this.pools) {
			if (this.pools.get(poolName) == null) {
				createPools(poolName, params);
			}
		}
		return this.pools.get(poolName).getConnection(this.waitTime, canShareConn);
	}

	private String getState(String poolName) {
		if (this.pools.get(poolName) == null) {
			return " pool not startup!";
		} else {
			return this.pools.get(poolName).getState();
		}
	}

	private void freeConnection(String poolName, FnConnection<?> conn, boolean releaseConn) {
		ConnectionPool pool = (ConnectionPool) this.pools.get(poolName);
		if (pool != null) {
			pool.freeConnection(conn, releaseConn);
		}
	}

	private void createPools(String poolName, HashMap<String, Object> params) {
		ConnectionPool pool = new ConnectionPool(GlobalParam.POOL_SIZE, poolName, params);
		this.pools.put(poolName, pool);
		log.info("success create pool " + poolName);
	}
 
	/**
	 * connection pools
	 * @author chengwen
	 * @version 2.0
	 * @date 2018-11-21 11:18
	 */
	private class ConnectionPool {
		private AtomicInteger activeNum = new AtomicInteger(0);
		private final int maxConn;
		private final String poolName;
		private final HashMap<String, Object> params;
		private ConcurrentLinkedQueue<FnConnection<?>> freeConnections = new ConcurrentLinkedQueue<FnConnection<?>>();
		private FnConnection<?> shareConn;

		public ConnectionPool(int maxConn, String poolName, final HashMap<String, Object> params) {
			super();
			if (params.containsKey("maxConn")) {
				this.maxConn = Integer.parseInt(String.valueOf(params.get("maxConn")));
			} else {
				this.maxConn = maxConn;
			}
			this.poolName = poolName;
			this.params = params;
			this.shareConn = newConnection();
			this.shareConn.setShare(true);
		}

		public FnConnection<?> getConnection(long timeout, boolean canShareConn) {
			FnConnection<?> conn = null;
			int tryTime = 0;
			while ((conn = getConnection()) == null) {
				if (canShareConn && conn == null) {
					if (this.shareConn.status() == false) {
						this.shareConn.connect();
					}
					return this.shareConn;
				}
				try {
					tryTime++;
					Thread.sleep(timeout);
				} catch (Exception e) {
					log.error(this.poolName + " Thread Exception", e);
				}
				if (tryTime > 10)
					break;
			}
			return conn;
		}

		public String getState() {
			return "Active Connections:" + activeNum + ",Free Connections:" + freeConnections.size()
					+ ",Max Connection:" + maxConn;
		}

		/**
		 * close connection pool all connections
		 */
		public void releaseAll() {
			synchronized(freeConnections) {
				for (FnConnection<?> conn : freeConnections) {
					if (!conn.free()) {
						log.warn("error close one connection in pool " + this.poolName);
					}
				}
				log.info("free connection pool " + this.poolName + " ,Active Connections:" + activeNum
						+ ",Release Connections:" + freeConnections.size());
				freeConnections.clear();
			} 
		}

		/**
		 * free connection and add to pool auto keep fixed connections
		 * 
		 * @param conn
		 *            free connection
		 * 
		 */
		private void freeConnection(FnConnection<?> conn, boolean releaseConn) {
			synchronized (this) {
				if (conn.isShare()) {
					if (releaseConn) {
						conn.free();
					}
				} else {
					if (releaseConn) {
						conn.free();
					} else {
						freeConnections.add(conn);
					}
					activeNum.decrementAndGet();
				}
			}
		}

		private FnConnection<?> getConnection() {
			synchronized (this) {
				FnConnection<?> conn = null;
				if (!freeConnections.isEmpty()) {
					conn = freeConnections.poll();
					while (conn.status() == false && !freeConnections.isEmpty()) {
						conn.free();
						conn = freeConnections.poll();
					}
					if (conn.status() == true) {
						activeNum.incrementAndGet();
						return conn;
					}
				}
				if (activeNum.get() < maxConn && (conn = newConnection()) != null) {
					activeNum.incrementAndGet();
					return conn;
				}
			}
			return null;
		}

		private FnConnection<?> newConnection() {
			FnConnection<?> conn = null;
			if (params != null) {
				switch ((DATA_TYPE) params.get("type")) {
				case ORACLE:
					conn = OracleConnection.getInstance(params);
					break;
				case MYSQL:
					conn = MysqlConnection.getInstance(params);
					break;
				case SOLR:
					conn = SolrConnection.getInstance(params);
					break;
				case ES:
					conn = ESConnection.getInstance(params);
					break;
				case HBASE:
					conn = HBaseConnection.getInstance(params);
					break;
				case ZOOKEEPER:
					conn = ZookeeperConnection.getInstance(params);
					break;
				default:
					log.error("Connection Type Not Support!");
					break;
				}
			} else {
				log.error("Parameter error can't create new " + this.poolName + " connection!");
			}
			return conn;
		}
	}
}