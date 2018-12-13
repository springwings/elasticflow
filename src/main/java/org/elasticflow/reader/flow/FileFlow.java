package org.elasticflow.reader.flow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.elasticflow.field.RiverField;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.reader.handler.Handler;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-22 09:33
 */
public class FileFlow extends ReaderFlowSocket {
	
	private final static Logger log = LoggerFactory.getLogger(FileFlow.class);
	
	public static FileFlow getInstance(HashMap<String, Object> connectParams) {
		FileFlow o = new FileFlow();
		o.INIT(connectParams);
		return o;
	}
	

	@Override
	public DataPage getPageData(HashMap<String, String> param, Map<String, RiverField> transParams, Handler handler,
			int pageSize) {
		PREPARE(false,false);
		boolean releaseConn = false;
		try {
			if(!ISLINK())
				return this.dataPage;
		} catch (Exception e) {
			releaseConn = true;
			log.error("get dataPage Exception", e);
		}finally{
			REALEASE(false,releaseConn);
		} 
		return this.dataPage;
	}

	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(HashMap<String, String> param, int pageSize) {
		ConcurrentLinkedDeque<String> dt = new ConcurrentLinkedDeque<>(); 
		PREPARE(false,false);
		if(!ISLINK())
			return dt; 
		boolean releaseConn = false;
		try {
			
		} catch (Exception e) {
			releaseConn = true;
			log.error("getPageSplit Exception", e);
		}finally{ 
			REALEASE(false,releaseConn);
		}
		return dt;
	} 

}
