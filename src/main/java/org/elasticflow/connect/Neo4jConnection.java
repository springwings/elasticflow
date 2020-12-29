package org.elasticflow.connect;

import java.sql.Connection;
import java.sql.DriverManager;

import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseSqlParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jConnection extends EFConnectionSocket<Connection> {

	private Connection conn = null;

	private final static Logger log = LoggerFactory.getLogger("Neo4j Socket");

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new Neo4jConnection();
		o.init(ConnectParams);
		o.connect();
		return o;
	}

	@Override
	public boolean connect() {
		try {
			if (!status()) {
				this.conn = DriverManager.getConnection(this.getConnectionUrl());
				log.info("build connect to " + getConnectionUrl());
			}
			return true;
		} catch (Exception e) {
			log.error(((WarehouseSqlParam) this.connectParams.getWhp()).getHost() + " connect Exception,", e);
			return false;
		}
	}

	@Override
	public Connection getConnection(boolean searcher) {
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
		try {
			if (this.conn != null && !this.conn.isClosed()) {
				return true;
			}
		} catch (Exception e) {
			log.error("get status Exception,", e);
		}
		return false;
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

	private String getConnectionUrl() {
		return "jdbc:neo4j:bolt://" + ((WarehouseSqlParam) this.connectParams.getWhp()).getHost() + "/?user="
				+ ((WarehouseSqlParam) this.connectParams.getWhp()).getUser() + ",password="
				+ ((WarehouseSqlParam) this.connectParams.getWhp()).getPassword()
				+ ",scheme=basic,failOverReadOnly=false";
	}

}
