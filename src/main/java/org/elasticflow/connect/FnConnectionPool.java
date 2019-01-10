package org.elasticflow.connect;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	public static FnConnectionSocket<?> getConn(ConnectParams params, String poolName, boolean canShareConn) {
		return FnCPool.getConnection(params, poolName, canShareConn);
	}

	public static void freeConn(FnConnectionSocket<?> conn, String poolName, boolean releaseConn) {
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
	private FnConnectionSocket<?> getConnection(ConnectParams params, String poolName, boolean canShareConn) {
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

	private void freeConnection(String poolName, FnConnectionSocket<?> conn, boolean releaseConn) {
		ConnectionPool pool = (ConnectionPool) this.pools.get(poolName);
		if (pool != null) {
			pool.freeConnection(conn, releaseConn);
		}
	}

	private void createPools(String poolName, ConnectParams params) {
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
		private final ConnectParams params; 
		private ConcurrentLinkedQueue<FnConnectionSocket<?>> freeConnections = new ConcurrentLinkedQueue<FnConnectionSocket<?>>();
		private FnConnectionSocket<?> shareConn;

		public ConnectionPool(int maxConn, String poolName, final ConnectParams params) {
			super();
			if (params.getWhp().getMaxConn()>0) {
				this.maxConn = params.getWhp().getMaxConn();
			} else {
				this.maxConn = maxConn;
			}
			this.poolName = poolName;
			this.params = params;
			this.shareConn = newConnection();
			this.shareConn.setShare(true);
		}

		public FnConnectionSocket<?> getConnection(long timeout, boolean canShareConn) {
			FnConnectionSocket<?> conn = null;
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
				for (FnConnectionSocket<?> conn : freeConnections) {
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
		private void freeConnection(FnConnectionSocket<?> conn, boolean releaseConn) {
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

		private FnConnectionSocket<?> getConnection() {
			synchronized (this) {
				FnConnectionSocket<?> conn = null;
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

		private FnConnectionSocket<?> newConnection() {
			FnConnectionSocket<?> conn = null;
			if (params != null) {
				switch (params.getWhp().getType()) {
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
				case FILE:
					conn = FileConnection.getInstance(params);
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