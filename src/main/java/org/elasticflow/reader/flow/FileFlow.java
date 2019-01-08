package org.elasticflow.reader.flow;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.elasticflow.config.GlobalParam;
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
		PREPARE(false, false);
		try {
			if (!ISLINK())
				return this.dataPage;
			RandomAccessFile rf = (RandomAccessFile) GETSOCKET().getConnection(false);
			int n = 3;
			while (n-- > 1) {
				rf.readLine();
			}
		} catch (Exception e) {
			log.error("get dataPage Exception", e);
		} finally {
			REALEASE(false, true);
		}
		return this.dataPage;
	}

	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(HashMap<String, String> param, int pageSize) {
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>(); 
		boolean releaseConn = false;
		try { 
			LineNumberReader lnr = new LineNumberReader(new FileReader(String.valueOf(GETSOCKET().getConnectParams().get("path"))));
			lnr.skip(Long.MAX_VALUE);
	        int lineNo = lnr.getLineNumber() + 1;
	        lnr.close();
	        for(int pos=0;lineNo-pos>GlobalParam.READ_PAGE_SIZE;pos+=GlobalParam.READ_PAGE_SIZE) {
	        	page.push(String.valueOf(pos));
	        }
	        page.push(String.valueOf(lineNo));
		} catch (Exception e) {
			releaseConn = true;
			log.error("getPageSplit Exception", e);
		} finally {
			REALEASE(false, releaseConn);
		}
		return page;
	}

}
