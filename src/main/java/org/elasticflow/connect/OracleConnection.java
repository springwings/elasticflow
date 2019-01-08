package org.elasticflow.connect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class OracleConnection extends FnConnectionSocket<Connection>{
	 
	private Connection conn = null;   
	
	private final static Logger log = LoggerFactory
			.getLogger("Oracle Socket");
	
	static{
		 try {
			Class.forName("oracle.jdbc.OracleDriver");
		} catch (Exception e) {
			log.error("OracleConnection Exception,",e);
		}
	}
	
	public static FnConnectionSocket<?> getInstance(HashMap<String, Object> ConnectParams){
		FnConnectionSocket<?> o = new OracleConnection();
		o.init(ConnectParams); 
		o.connect();
		return o;
	} 

	@Override
	public boolean connect() {
		try {
			if (!status()) {
				this.conn = DriverManager.getConnection(getConnectionUrl(),
						String.valueOf(this.connectParams.get("user")), String.valueOf(this.connectParams.get("password")));
				log.info("build connect to the Database " + this.connectParams.get("host")
						+ this.connectParams.get("port") + this.connectParams.get("dbname"));
			}
			return true;
		} catch (Exception e) {
			log.error("connect Exception,", e);
			return false;
		}
	}

	@Override
	public Connection getConnection(boolean searcher) {
		int tryTime=0;
		try {
			while(tryTime<5 && !connect()){ 
				tryTime++;
				Thread.sleep(2000); 
			} 
		} catch (Exception e) {
			log.error("try to get Connection Exception,", e);
		}
		return this.conn;
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
	
	@Override
	public boolean status(){
		try {
			if(this.conn != null && !this.conn.isClosed() ){
				return true;
			} 
		} catch (Exception e) { 
			log.error("get status Exception,", e);
		} 
		return false;
	} 
	
	private String getConnectionUrl() {
		return "jdbc:oracle:thin:@" + this.connectParams.get("host") + ":" + this.connectParams.get("port") + "/" + this.connectParams.get("sid");		 
	}

}
