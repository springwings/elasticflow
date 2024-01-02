package org.elasticflow.connection.sockets;

import java.sql.Connection;
import java.sql.DriverManager;

import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.connection.EFConnectionSocket;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mysql basic connection establishment management class
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class MysqlConnection extends EFConnectionSocket<Connection> {

	private final static Logger log = LoggerFactory.getLogger("Mysql Socket");

	static {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
		} catch (Exception e) {
			log.error("mysql connection class init exception", e);
		}
	}

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new MysqlConnection();
		o.init(ConnectParams);
		return o;
	}

	@Override
	public boolean connect(END_TYPE endType) {
		try {
			if (!status()) {
				WarehouseParam wsp = this.connectParams.getWhp();
				this.conn = DriverManager.getConnection(getConnectionUrl(),
						wsp.getUser(),
						wsp.getPassword()); 
			}
			return true;
		} catch (Exception e) {
			log.error("{} mysql {} connect exception", this.connectParams.getWhp().getAlias(),endType.name(),e);
			return false;
		}
	}

	@Override
	public boolean free() {
		try {
			if(this.conn!=null)
				this.conn.close();
			this.conn = null;
			this.connectParams = null;
		} catch (Exception e) {
			log.warn("{} free mysql connection exception", this.connectParams.getWhp().getAlias(),e);
			return false;
		}
		return true;
	}

	@Override
	public boolean status() {
		try {
			if (this.conn != null && !this.conn.isClosed()) {
				return true;
			}
		} catch (Exception e) {
			log.error("{} get mysql status exception", this.connectParams.getWhp().getAlias(),e);
		}
		return false;
	}

	private String getConnectionUrl() {
		return "jdbc:mysql://" +this.connectParams.getWhp().getHost() + ":"
				+ this.connectParams.getWhp().getPort() + "/"
				+ this.connectParams.getWhp().getL1name(this.connectParams.getL1Seq())
				+ "?autoReconnect=true&failOverReadOnly=false&useSSL=false";
	}
}
