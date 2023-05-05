package org.elasticflow.connection;

import java.sql.Connection;
import java.sql.DriverManager;

import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Neo4j basic connection establishment management class
 * use bolt protocol
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class Neo4jConnection extends EFConnectionSocket<Connection> {

	private final static Logger log = LoggerFactory.getLogger("Neo4j Socket");

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new Neo4jConnection();
		o.init(ConnectParams);
		return o;
	}

	@Override
	protected boolean connect(END_TYPE endType) {
		try {
			if (!status()) {
				this.conn = DriverManager.getConnection(this.getConnectionUrl());
				log.info("build connect to " + getConnectionUrl());
			}
			return true;
		} catch (Exception e) {
			log.error(this.connectParams.getWhp().getHost() + " connect Exception,", e);
			return false;
		}
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
			log.warn("free connect Exception,", e);
			return false;
		}
		return true;
	}

	private String getConnectionUrl() {
		return "jdbc:neo4j:bolt://" + this.connectParams.getWhp().getHost() + ":"+
				this.connectParams.getWhp().getPort()+ "/?user="
				+ this.connectParams.getWhp().getUser() + ",password="
				+ this.connectParams.getWhp().getPassword()
				+ ",scheme=basic,failOverReadOnly=false";
	}

}
