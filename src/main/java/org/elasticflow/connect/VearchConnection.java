package org.elasticflow.connect;

import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseNosqlParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2021-07-09 09:25
 */
public class VearchConnection extends EFConnectionSocket<VearchConnector> {

	private VearchConnector conn = null;

	private final static Logger log = LoggerFactory.getLogger("Vearch Socket");
	
	final String DEAFULT_KEY = "dbname";
	
	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new VearchConnection();
		o.init(ConnectParams);
		o.connect();
		return o;
	}

	@Override
	public boolean connect() {
		WarehouseNosqlParam wnp = (WarehouseNosqlParam) this.connectParams.getWhp();
		if (wnp.getPath() != null) {
			if (!status()) { 
				String[] paths = wnp.getPath().split("#");
				this.conn = new VearchConnector(paths[0],paths[1],wnp.getDefaultValue().getString(DEAFULT_KEY));
			}
		} else {
			return false;
		}
		return true;
	}

	@Override
	public VearchConnector getConnection(END_TYPE endType) {
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
		if (this.conn == null) {
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
