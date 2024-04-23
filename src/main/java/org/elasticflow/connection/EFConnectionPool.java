package org.elasticflow.connection;

import java.lang.reflect.Method;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection management of various resources
 * 
 * @author chengwen
 * @version 3.0
 * @date 2018-11-21 11:02 
 */
@ThreadSafe
public final class EFConnectionPool {
	
	/**Connection pool control center instance*/
	private final static EFConnectionPool EFCPool;
	
	/**All Connection pool control centers of the system*/
	private ConcurrentHashMap<String, ConnectionPool> _GPOOLS = new ConcurrentHashMap<>();
	
	/**re-connect wait time */
	private int _waitTime = 1000;

	private final static Logger log = LoggerFactory.getLogger("EFConnectionPool");

	static {
		EFCPool = new EFConnectionPool();
	}
 
	/**
	 * External access to obtain connections
	 * @param params
	 * @param poolName
	 * @param acceptShareConn
	 * @return
	 */
	public static EFConnectionSocket<?> getConn(ConnectParams params, String poolName, boolean acceptShareConn) {
		return EFCPool.getConnection(params, poolName, acceptShareConn);
	}

	/**
	 * Entrance for external release connection
	 * @param conn         the instance of connection
	 * @param poolName     Connection pool name
	 * @param clearConn    Whether to clean up the connection while releasing it
	 */
	public static void freeConn(EFConnectionSocket<?> conn, String poolName, boolean clearConn) {
		EFCPool.freeConnection(poolName, conn, clearConn);
	}
	
	/**
	 * Get the specified Connection pool status
	 * @param poolName Connection pool name
	 * @return
	 */
	public static String getStatus(String poolName) {
		return EFCPool.getState(poolName);
	}

	/**
	 * clear the specified Connection pool status
	 * @param poolName Connection pool name
	 */
	public static void clearPool(String poolName) {
		EFCPool._clearPool(poolName);
	}

	/**
	 * Clear all connections in the resource pool
	 * @param poolName Connection pool name
	 */
	private void _clearPool(String poolName) {
		if (poolName != null) {
			if (this._GPOOLS.containsKey(poolName))
				this._GPOOLS.get(poolName).releaseAll();
		} else {
			for (Entry<String, ConnectionPool> ent : this._GPOOLS.entrySet()) {
				ent.getValue().releaseAll();
			}
		}
	}

	/**
	 * get connection from pool and waiting
	 * Lazy loading mode
	 */
	private EFConnectionSocket<?> getConnection(ConnectParams params, String poolName, boolean acceptShareConn) {
		if (this._GPOOLS.get(poolName) == null) {
			createPools(poolName, params);
		}
		return this._GPOOLS.get(poolName).getConnection(this._waitTime, acceptShareConn);
	}

	private String getState(String poolName) {
		if (this._GPOOLS.get(poolName) == null) {
			return " pool not startup!";
		} else {
			return this._GPOOLS.get(poolName).getState();
		}
	}
	/**
	 * Release resource possession
	 */
	private void freeConnection(String poolName, EFConnectionSocket<?> conn, boolean clearConn) {
		ConnectionPool pool = (ConnectionPool) this._GPOOLS.get(poolName);
		if (pool != null && conn!=null) {
			pool.freeConnection(conn, clearConn);
		}
	}

	private void createPools(String poolName, ConnectParams params) {
		ConnectionPool pool = new ConnectionPool(poolName, params);
		this._GPOOLS.put(poolName, pool);
		log.info("success create pool {}",poolName);
	}
 
	
	
	/**
	 * Connection pool management class
	 * @author chengwen
	 * @version 2.0
	 * @date 2018-11-21 11:18
	 */
	private class ConnectionPool {
		/**The current number of active connections. These connections are unmanageable in the Connection pool*/
		private AtomicInteger activeNum = new AtomicInteger(0);
		/**Control the number of connections created*/
		private int maxConn;
		/**Unique ID of the Connection pool. The main part is determined by the name in the external configuration*/
		private final String poolName;
		private final ConnectParams params; 
		/**Connect Resource Storage Recycle Pool*/
		private ConcurrentLinkedQueue<EFConnectionSocket<?>> connectionPools = new ConcurrentLinkedQueue<EFConnectionSocket<?>>();
		/**Shared connection, accessible to anyone*/
		private EFConnectionSocket<?> shareConn;
		/**Current resource pool version*/
		private long version = Common.getNow();

		public ConnectionPool(String poolName, final ConnectParams params) {
			super();
			if (params.getWhp().getMaxPoolSize()>0) {
				this.maxConn = params.getWhp().getMaxPoolSize();
			} else {
				this.maxConn = GlobalParam.CONNECTION_POOL_SIZE;
			}
			this.poolName = poolName;
			this.params = params;
			this.shareConn = newConnection();
			this.shareConn.setShare(true);
		}
		
		public long getVersion() {
			return this.version;
		}

		public EFConnectionSocket<?> getConnection(long timeout, boolean acceptShareConn) {
			EFConnectionSocket<?> conn = null;
			int tryTime = 0;
			while ((conn = getConnection()) == null) {
				if (acceptShareConn && conn == null) {
					if (this.shareConn.status() == false) {
						this.shareConn.connect(END_TYPE.searcher);
					}
					return this.shareConn;
				}
				try {
					tryTime++;
					Thread.sleep(timeout);
				} catch (Exception e) {
					log.error("{} thread sleep exception",this.poolName,e);
				}
				if (tryTime > 10)
					break;
			}
			if(conn==null)
				log.warn("{} Connection pool is full, unable to get connection.",this.poolName);
			return conn;
		}

		public String getState() {
			return "Active Connections:" + activeNum + ",Free Connections:" + connectionPools.size()
					+ ",Max Connection:" + maxConn;
		}

		/**
		 * Release all connections in the resource pool
		 */
		public void releaseAll() { 
			for (EFConnectionSocket<?> conn : connectionPools) {
				if (!conn.free()) {
					log.warn("connection cleaning encountered an error ",conn);
				}
			}
			log.info("free connection pool {},Active Connections:{},Release Connections:{}",this.poolName,activeNum,connectionPools.size()); 
			connectionPools.clear();
			this.version = Common.getNow();
		}

		/**
		 * Intelligently determine whether to recycle connections to the resource pool
		 * @param conn  connection socker
		 * @param releaseConn Determine whether to discard the connection
		 * 
		 */
		private void freeConnection(EFConnectionSocket<?> conn, boolean clearConn) {
			synchronized (this) {
				if (conn.isShare()) {
					if (clearConn) {
						conn.free();
					}
				} else {
					if (clearConn) {
						Common.LOG.info("clear connection {}.",conn);
						conn.free();						
					} else { 
						if(conn.getVersion()==getVersion())
							connectionPools.add(conn);
					}
					activeNum.decrementAndGet();
				}
			}
		}

		private EFConnectionSocket<?> getConnection() {
			synchronized (this) {
				EFConnectionSocket<?> conn = null;
				if (!connectionPools.isEmpty()) {
					conn = connectionPools.poll();
					while (conn.status() == false && !connectionPools.isEmpty()) {
						conn.free();
						conn = connectionPools.poll();
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
				String _class_name;
				if (params.getWhp().getHandler() != null) {
					_class_name = params.getWhp().getHandler();
				} else {
					_class_name = "org.elasticflow.connection.sockets."+Common.changeFirstCase(params.getWhp().getType().name().toLowerCase())+"Connection";
				}
				try {					
					Class<?> clz = Class.forName(_class_name); 
					Method m = clz.getMethod("getInstance", ConnectParams.class);  
					conn = (EFConnectionSocket<?>) m.invoke(null,params);
					conn.setVersion(this.version);
				}catch (Exception e) { 
					log.error("connection class {} not exist!",_class_name,e);
				}
			} else {
				log.error("{} connection parameters is null!",this.poolName);
			}
			return conn;
		}
	}
}