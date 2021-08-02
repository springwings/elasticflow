package org.elasticflow.connect;

import java.lang.reflect.Method;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection management of various resources
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-21 11:02
 */
@ThreadSafe
public final class EFConnectionPool {

	private final static EFConnectionPool EFCPool;

	private ConcurrentHashMap<String, ConnectionPool> _pools = new ConcurrentHashMap<>();
	
	/**re-connect wait time */
	private int _waitTime = 1000;

	private final static Logger log = LoggerFactory.getLogger("EFConnectionPool");

	static {
		EFCPool = new EFConnectionPool();
	}

	/**
	 * @param canShareConn ,Control whether the connection can share processing data
	 * @return
	 */
	public static EFConnectionSocket<?> getConn(ConnectParams params, String poolName, boolean canShareConn) {
		return EFCPool.getConnection(params, poolName, canShareConn);
	}

	public static void freeConn(EFConnectionSocket<?> conn, String poolName, boolean releaseConn) {
		EFCPool.freeConnection(poolName, conn, releaseConn);
	}

	public static String getStatus(String poolName) {
		return EFCPool.getState(poolName);
	}

	public static void release(String poolName) {
		EFCPool.releasePool(poolName);
	}

	/**
	 * release pools
	 */
	private void releasePool(String poolName) {
		synchronized (this._pools) {
			if (poolName != null) {
				if (this._pools.containsKey(poolName))
					this._pools.get(poolName).releaseAll();
			} else {
				for (Entry<String, ConnectionPool> ent : this._pools.entrySet()) {
					ent.getValue().releaseAll();
				}
			}
		}
	}

	/**
	 * get connection from pool and waiting
	 */
	private EFConnectionSocket<?> getConnection(ConnectParams params, String poolName, boolean canShareConn) {
		synchronized (this._pools) {
			if (this._pools.get(poolName) == null) {
				createPools(poolName, params);
			}
		}
		return this._pools.get(poolName).getConnection(this._waitTime, canShareConn);
	}

	private String getState(String poolName) {
		if (this._pools.get(poolName) == null) {
			return " pool not startup!";
		} else {
			return this._pools.get(poolName).getState();
		}
	}

	private void freeConnection(String poolName, EFConnectionSocket<?> conn, boolean releaseConn) {
		ConnectionPool pool = (ConnectionPool) this._pools.get(poolName);
		if (pool != null) {
			pool.freeConnection(conn, releaseConn);
		}
	}

	private void createPools(String poolName, ConnectParams params) {
		ConnectionPool pool = new ConnectionPool(GlobalParam.POOL_SIZE, poolName, params);
		this._pools.put(poolName, pool);
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
		private ConcurrentLinkedQueue<EFConnectionSocket<?>> freeConnections = new ConcurrentLinkedQueue<EFConnectionSocket<?>>();
		private EFConnectionSocket<?> shareConn;

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

		public EFConnectionSocket<?> getConnection(long timeout, boolean canShareConn) {
			EFConnectionSocket<?> conn = null;
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
				for (EFConnectionSocket<?> conn : freeConnections) {
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
		private void freeConnection(EFConnectionSocket<?> conn, boolean releaseConn) {
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

		private EFConnectionSocket<?> getConnection() {
			synchronized (this) {
				EFConnectionSocket<?> conn = null;
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

		private EFConnectionSocket<?> newConnection() {
			EFConnectionSocket<?> conn = null;
			if (params != null) {
				String _class_name = "org.elasticflow.connect."+Common.changeFirstCase(params.getWhp().getType().name().toLowerCase())+"Connection";
				try {					
					Class<?> clz = Class.forName(_class_name); 
					Method m = clz.getMethod("getInstance", ConnectParams.class);  
					conn = (EFConnectionSocket<?>) m.invoke(null,params);
				}catch (Exception e) { 
					log.error(_class_name+" Not Support!",e);
				}
			} else {
				log.error("Parameter error can't create new " + this.poolName + " connection!");
			}
			return conn;
		}
	}
}