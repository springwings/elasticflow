package org.elasticflow.reader.flow;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.model.Page;
import org.elasticflow.model.Task;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.ReaderFlowSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-22 09:33
 */
public class FilesReader extends ReaderFlowSocket {

	private final static Logger log = LoggerFactory.getLogger(FilesReader.class);

	public static FilesReader getInstance(ConnectParams connectParams) {
		FilesReader o = new FilesReader();
		o.initConn(connectParams);
		return o;
	}

	@Override
	public DataPage getPageData(final Page page,int pageSize) {
		PREPARE(false, false);
		try {
			if (!ISLINK())
				return this.dataPage;
			RandomAccessFile rf = (RandomAccessFile) GETSOCKET().getConnection(END_TYPE.reader); 
			int start = Integer.parseInt(page.getStart());
			int pos = 0;
			while (pos++ > 1) {
				if(pos<start) {
					rf.readLine();
				}else {
					PipeDataUnit u = PipeDataUnit.getInstance();
					PipeDataUnit.addFieldValue("id",rf.readLine(), page.getInstanceConfig().getReadFields(),u);
					this.dataUnit.add(u);
					if(pos>start+pageSize)
						break;
				} 
			}
			this.dataPage.putData(this.dataUnit);
			this.dataPage.put(GlobalParam.READER_STATUS,true);
		} catch (Exception e) {
			log.error("get dataPage Exception", e);
		} finally {
			REALEASE(false, true);
		}
		return this.dataPage;
	}

	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(final Task task,int pageSize) {
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>(); 
		boolean releaseConn = false;
		PREPARE(false, false);
		if (!ISLINK())
			return page;
		try { 
			LineNumberReader lnr = new LineNumberReader(
					new FileReader(GETSOCKET().getConnectParams().getWhp().getHost()));
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
