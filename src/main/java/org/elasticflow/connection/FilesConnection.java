package org.elasticflow.connection;

import java.util.ArrayList;

import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.EFFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Files basic connection establishment management class
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */

public class FilesConnection extends EFConnectionSocket<ArrayList<String>> { 

	private final static Logger log = LoggerFactory.getLogger("File Socket");
	
	final static String EXTENSION = "extension";
 
	public static EFConnectionSocket<?> getInstance(ConnectParams connectParams){
		EFConnectionSocket<?> o = new FilesConnection();
		o.init(connectParams);  
		return o;
	}
	
	@Override
	protected boolean connect(END_TYPE endType) {
		if (!status()) {
			try { 
				if(connectParams.getWhp().getCustomParams()!=null) {
					this.conn = EFFileUtil.scanFolder(String.valueOf(connectParams.getWhp().getHost()),connectParams.getWhp().getCustomParams().getString(EXTENSION));
				}else {
					this.conn = EFFileUtil.scanFolder(String.valueOf(connectParams.getWhp().getHost()),null);
				}				
				return true;
			} catch (Exception e) {
				 log.error("{} folder {} scan exception",connectParams.getWhp().getAlias(),endType.name(),e);
			}
		} 
		return false;
	}
	 
	@Override
	public boolean status() {
		if(this.conn != null) {
			return true;
		}
		return false;
	}

	@Override
	public boolean free() {
		try {
			if(this.conn!=null)
				this.conn.clear();
			this.conn = null;
		} catch (Exception e) {
			 log.warn("{} free file connection exception", this.connectParams.getWhp().getAlias(),e);
		}
		return false;
	} 
}