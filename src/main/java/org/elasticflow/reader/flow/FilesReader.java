package org.elasticflow.reader.flow;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.model.Page;
import org.elasticflow.model.Task;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.util.EFException;
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
	
	private String filePath;

	public static FilesReader getInstance(ConnectParams connectParams) {
		FilesReader o = new FilesReader();
		o.initConn(connectParams);
		return o;
	} 

	@Override
	public DataPage getPageData(final Page page,int pageSize) throws EFException {
		PREPARE(false, false);
		try {
			if (!ISLINK())
				return this.dataPage;
			if(this.readHandler==null){
				try(RandomAccessFile rf = new RandomAccessFile(filePath, "r");){ 
					int pos = 0;
					rf.seek(Integer.parseInt(page.getStart()));
					while (pos<pageSize) {
						PipeDataUnit u = PipeDataUnit.getInstance();
						PipeDataUnit.addFieldValue("datas",rf.readLine(), page.getInstanceConfig().getReadFields(),u);
						this.dataUnit.add(u);
						pos++;
					}
				}
				this.dataPage.putData(this.dataUnit);
				this.dataPage.put(GlobalParam.READER_STATUS,true);
			}else {
				this.readHandler.handleData(this,filePath,page,pageSize);
			}
			
		} catch (Exception e) {
			throw new EFException(e);	
		} finally {
			REALEASE(false, true);
		}
		return this.dataPage;
	}

	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(final Task task,int pageSize) throws EFException {
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>(); 
		boolean releaseConn = false;
		PREPARE(false, false);
		if (!ISLINK())
			return page;
		try { 
			Long startTime = Long.valueOf(task.getStartTime());
			Long minTime = 0L;
			filePath = null;
			@SuppressWarnings("unchecked")
			ArrayList<String> paths = (ArrayList<String>) GETSOCKET().getConnection(END_TYPE.reader); 
			for(String path : paths) {  
		        File file = new File(path);
		        Long lastModified = file.lastModified();
		        if(lastModified>startTime) {
		        	if(minTime==0 || minTime>lastModified) {
		        		filePath = path;
		        		minTime = lastModified;
		        	}
		        }
			}
			if(filePath!=null) {
				LineNumberReader lnr = new LineNumberReader(
						new FileReader(filePath));
				lnr.skip(Long.MAX_VALUE);
		        int lineNo = lnr.getLineNumber() + 1;
		        lnr.close();
		        for(int pos=0;lineNo-pos>GlobalParam.READ_PAGE_SIZE;pos+=GlobalParam.READ_PAGE_SIZE) {
		        	page.push(String.valueOf(pos));
		        }
		        page.push(String.valueOf(lineNo));
			}	       
		} catch (Exception e) {
			releaseConn = true;
			this.dataPage.put(GlobalParam.READER_STATUS,false);
			log.error("{} file get dataPage Exception will auto free connection!",task.getInstanceID(),e);
			throw new EFException("Files Reader get dataPage Exception!");	
		} finally {
			REALEASE(false, releaseConn);
		}
		return page;
	}

}
