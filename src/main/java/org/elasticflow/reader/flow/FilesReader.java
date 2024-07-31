package org.elasticflow.reader.flow;

import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.model.task.TaskCursor;
import org.elasticflow.model.task.TaskModel;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Files reader mainly consists of two parts: pagination scan and detailed
 * content read
 * 
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
	 * Can only be two fixed fields title,content It will generate a unique ID based
	 * on the title field hash
	 * 
	 * @param page
	 * @param pageSize
	 * @throws Exception
	 */
	private void processTxt(TaskCursor page, int pageSize) throws Exception {
		int pos = 0;
		String LAST_STAMP = null;
		for (String fpath : this.filePath) {
			PipeDataUnit u = PipeDataUnit.getInstance();
			String content = EFFileUtil.readText(fpath, GlobalParam.ENCODING, false);
			String title = EFFileUtil.getFileObj(fpath).getName();
			String id = UUID.nameUUIDFromBytes((title).getBytes()).toString().replace("-", "");
			PipeDataUnit.addFieldValue("id", id, page.getInstanceConfig().getReadFields(), u);
			PipeDataUnit.addFieldValue("title", title, page.getInstanceConfig().getReadFields(), u);
			PipeDataUnit.addFieldValue("content", content, page.getInstanceConfig().getReadFields(), u);
			LAST_STAMP = String.valueOf(EFFileUtil.getFileObj(fpath).lastModified());
			u.setReaderKeyVal(id);
			this.dataUnit.add(u);
		}
		this.filePath.clear();
		if (LAST_STAMP == null) {
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis());
		} else {
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, LAST_STAMP);
		}
		this.dataPage.putData(this.dataUnit);
		this.dataPage.putDataBoundary(String.valueOf(Integer.parseInt(page.getStart()) + pos));
	}

	/**
	 * csv header map to fields define
	 * 
	 * @param page
	 * @param pageSize
	 * @return
	 * @throws Exception
	 */
	private void processCsv(TaskCursor page, int pageSize) throws Exception {
		int pos = 0;
		String LAST_STAMP = null;
		if (this.filePath.size() == 1) {
			String[] headers;
			try (RandomAccessFile rf = new RandomAccessFile(this.filePath.get(0), "r");) {
				headers = rf.readLine().split(",");
				int startpos = Integer.parseInt(page.getStart());
				if (startpos > 0)
					rf.seek(startpos);
				while (pos < pageSize) {
					String line = rf.readLine();
					if (line != null) {
						line = new String(line.getBytes("ISO-8859-1"), GlobalParam.ENCODING); 
						String[] datas = line.strip().split(",");
						if(headers.length!=datas.length) {
							log.warn("dirty data headers length {},datas length {}, source {}",headers.length,datas.length,line);
							continue;
						}
						PipeDataUnit u = PipeDataUnit.getInstance();
						for (int i = 0; i < headers.length; i++) {
							PipeDataUnit.addFieldValue(headers[i], datas[i], page.getInstanceConfig().getReadFields(),
									u);
							if (headers[i].equals(this.dataPage.get(GlobalParam.READER_SCAN_KEY))) {
								LAST_STAMP = datas[i];
							} else if (headers[i].equals(this.dataPage.get(GlobalParam.READER_KEY))) {
								u.setReaderKeyVal(datas[i]);
							}
						}
						this.dataUnit.add(u);
						pos++;
					} else {
						break;
					}
				}
			}
		} else {
			List<String> headers = new ArrayList<>();
			for (String fpath : this.filePath) {
				try (Reader reader = new FileReader(fpath);
						CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {
					for (CSVRecord csvRecord : csvParser) {  
						if (csvParser.getCurrentLineNumber() == 1) {
							headers = csvRecord.toList();
						} else { 
							List<String> rowData = csvRecord.toList();
							if(headers.size()!=rowData.size()) {
								log.warn("dirty data, headers length {},datas length {}, source {}",headers.size(),rowData.size(),csvRecord);
								continue;
							}
							PipeDataUnit u = PipeDataUnit.getInstance();
							for (int i = 0; i < headers.size(); i++) {
								PipeDataUnit.addFieldValue(headers.get(i), rowData.get(i),
										page.getInstanceConfig().getReadFields(), u);
								if (headers.get(i).equals(this.dataPage.get(GlobalParam.READER_SCAN_KEY))) {
									LAST_STAMP = rowData.get(i);
								} else if (headers.get(i).equals(this.dataPage.get(GlobalParam.READER_KEY))) {
									u.setReaderKeyVal(rowData.get(i));
								}
							}
							this.dataUnit.add(u);
						}
					}
				}  
			}
			this.filePath.clear();
			pos = 0;
		}
		if (LAST_STAMP == null) {
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis());
		} else {
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, LAST_STAMP);
		}
		this.dataPage.putData(this.dataUnit);
		this.dataPage.putDataBoundary(String.valueOf(Integer.parseInt(page.getStart()) + pos));
	}

	/**
	 * Can only support single file type processing
	 */
	@Override
	public DataPage getPageData(final TaskCursor taskCursor, int pageSize) throws EFException {
		PREPARE(false, false);
		try {
			if (!connStatus())
				return this.dataPage;
			this.dataPage.put(GlobalParam.READER_KEY, taskCursor.getReaderKey());
			this.dataPage.put(GlobalParam.READER_SCAN_KEY, taskCursor.getReaderScanKey());
			if (this.readHandler == null) {
				switch (EFFileUtil.getFileExtension(this.filePath.get(0))) {
				case "csv":
					this.processCsv(taskCursor, pageSize);
					break;
				case "txt":
					this.processTxt(taskCursor, pageSize);
					break;
				default:
					break;
				}
				this.dataPage.putData(this.dataUnit);
				this.dataPage.put(GlobalParam.READER_STATUS, true);
			} else {
				this.readHandler.handleData(this, filePath, taskCursor, pageSize);
			}
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, scanTime);
		} catch (Exception e) {
			throw new EFException(e);
		} finally {
			releaseConn(false, true);
		}
		return this.dataPage;
	}

	@Override
	public ConcurrentLinkedDeque<String> getDataPages(final TaskModel task, int pageSize) throws EFException {
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();
		boolean clearConn = false;
		PREPARE(false, false);
		if (!connStatus())
			return page;
		try {
			Long startTime = 0L;
			if (task.getStartTime().length() > 0)
				startTime = Long.valueOf(task.getStartTime());
			this.scanTime = startTime;
			this.filePath.clear();
			@SuppressWarnings("unchecked")
			ArrayList<String> paths = (ArrayList<String>) GETSOCKET().getConnection(END_TYPE.reader);
			if (paths.size() == 1) {
				Long lastModified = EFFileUtil.getFileObj(paths.get(0)).lastModified();
				if (lastModified > startTime) {
					this.filePath.add(paths.get(0));
					this.scanTime = lastModified;
				}
				if (this.filePath.size() > 0) {
					RandomAccessFile rf = new RandomAccessFile(filePath.get(0), "r");
					rf.seek(0);
					int pos = 0;
					page.push("0");
					while (rf.readLine() != null) {
						pos += 1;
						if (pos % pageSize == 0)
							page.push(String.valueOf(rf.getFilePointer()));
					}
					rf.close();
				}
			} else if (paths.size() > 1) {
				for (String path : paths) {
					File file = new File(path);
					Long lastModified = file.lastModified();
					if (lastModified > startTime) {
						this.filePath.add(path);
						this.scanTime = lastModified;
					}
				}
				if (this.filePath.size() > 0)
					page.push("0");
			}

		} catch (Exception e) {
			clearConn = true;
			this.dataPage.put(GlobalParam.READER_STATUS, false); 
			throw new EFException(e,task.getInstanceID()+ " Files Reader get dataPage Exception!");
		} finally {
			releaseConn(false, clearConn);
		}
		return page;
	}

}
