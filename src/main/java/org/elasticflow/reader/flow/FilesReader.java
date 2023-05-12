package org.elasticflow.reader.flow;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
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
 * Files reader mainly consists of two parts: pagination scan and detailed content read
 * @author chengwen
 * @version 1.0
 * @date 2018-11-22 09:33
 */
public class FilesReader extends ReaderFlowSocket {

	private final static Logger log = LoggerFactory.getLogger(FilesReader.class);

	private List<String> filePath = new ArrayList<>();
	
	private Long scanTime;

	public static FilesReader getInstance(ConnectParams connectParams) {
		FilesReader o = new FilesReader();
		o.initConn(connectParams);
		return o;
	}
	
	/**
	 * csv header map to fields define
	 * @param page
	 * @param pageSize
	 * @return
	 * @throws Exception
	 */
	private int processCsv(Page page, int pageSize) throws Exception {
		int pos = 0;
		String[] headers;
		if(this.filePath.size()==1) { 
			try (RandomAccessFile rf = new RandomAccessFile(this.filePath.get(0), "r");) {
				headers = rf.readLine().split(",");
				int startpos = Integer.parseInt(page.getStart());
				if (startpos>0) 
					rf.seek(startpos);
				while (pos < pageSize) {
					String line = rf.readLine();
					if (line != null) {  
						line = new String(line.getBytes("ISO-8859-1"), GlobalParam.ENCODING);
						PipeDataUnit u = PipeDataUnit.getInstance();
						String[] datas = line.strip().split(",");
						for(int i=0;i<headers.length;i++) {
							PipeDataUnit.addFieldValue(headers[i], datas[i], page.getInstanceConfig().getReadFields(),
									u);
						} 
						this.dataUnit.add(u);
						pos++;
					} else {
						break;
					}
				}
			}
		}else {
			for(String fpath :this.filePath) {
				try (RandomAccessFile rf = new RandomAccessFile(fpath, "r");) {
					headers = rf.readLine().split(","); 
					String line = rf.readLine();
					while(line !=null) {
						PipeDataUnit u = PipeDataUnit.getInstance();
						String[] datas = line.strip().split(",");
						for(int i=0;i<headers.length;i++) {
							PipeDataUnit.addFieldValue(headers[i], datas[i], page.getInstanceConfig().getReadFields(),
									u);
						} 
						this.dataUnit.add(u);
						line = rf.readLine();
					} 
				}
			}
			this.filePath.clear();
			pos = 0;
		}	
		return pos;
	}

	@Override
	public DataPage getPageData(final Page page, int pageSize) throws EFException {
		PREPARE(false, false, false);
		try {
			if (!ISLINK())
				return this.dataPage;
			if (this.readHandler == null) {
				int pos = this.processCsv(page, pageSize); 
				this.dataPage.putData(this.dataUnit);
				this.dataPage.put(GlobalParam.READER_STATUS, true);
				this.dataPage.put(GlobalParam.READER_SCAN_KEY, page.getReaderScanKey());
				this.dataPage.putDataBoundary(String.valueOf(Integer.parseInt(page.getStart()) + pos));
			} else {
				this.readHandler.handleData(this, filePath, page, pageSize);
			}
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, scanTime); 
		} catch (Exception e) {
			throw new EFException(e);
		} finally {
			REALEASE(false, true);
		}
		return this.dataPage;
	}

	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(final Task task, int pageSize) throws EFException {
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();
		boolean releaseConn = false;
		PREPARE(false, false, false);
		if (!ISLINK())
			return page;
		try {
			Long startTime = 0L;
			if(task.getStartTime().length()>0)
				startTime =  Long.valueOf(task.getStartTime());
			this.scanTime = startTime;
			this.filePath.clear();
			@SuppressWarnings("unchecked")
			ArrayList<String> paths = (ArrayList<String>) GETSOCKET().getConnection(END_TYPE.reader);
			if(paths.size()==1) {
				File file = new File(paths.get(0));
				Long lastModified = file.lastModified();
				if (lastModified > startTime) {
					this.filePath.add(paths.get(0));
					this.scanTime = lastModified;
				} 
				if (this.filePath.size()>0) {
					RandomAccessFile rf = new RandomAccessFile(filePath.get(0), "r");
					rf.seek(0); 
					int pos = 0;
					page.push("0");
		            while (rf.readLine() != null) {
		            	pos+=1;	            		
		            	if(pos%pageSize==0)
		            		page.push(String.valueOf(rf.getFilePointer()));	            	
		            }	
		            rf.close();
				}
			}else if(paths.size()>1) {
				for (String path : paths) {
					File file = new File(path);
					Long lastModified = file.lastModified();
					if (lastModified > startTime) {
						this.filePath.add(path);
						this.scanTime = lastModified;
					}
				}
				if(this.filePath.size()>0)
					page.push("0");
			}
			
		} catch (Exception e) {
			releaseConn = true;
			this.dataPage.put(GlobalParam.READER_STATUS, false);
			log.error("{} file get dataPage Exception will auto free connection!", task.getInstanceID(), e);
			throw new EFException("Files Reader get dataPage Exception!");
		} finally {
			REALEASE(false, releaseConn);
		}
		return page;
	}

}
