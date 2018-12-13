package org.elasticflow.reader.util;

import java.util.LinkedList;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit; 

/**
 *  * pass data set in argument,writer will auto get each line
 * @author chengwen
 * @version 2.0
 * @date 2018-10-12 14:32
 */
public class DataSetReader{  
	private String IncrementColumn;
	private String keyColumn;
	private String READER_LAST_STAMP = "";
	private String dataBoundary;
	private LinkedList<PipeDataUnit> datas;
	private boolean status = true;
 
	public void init(DataPage DP) {
		if (DP.size() > 1) {
			this.keyColumn =  String.valueOf(DP.get(GlobalParam.READER_KEY));
			this.IncrementColumn = String.valueOf(DP.get(GlobalParam.READER_SCAN_KEY));
			this.dataBoundary = DP.getDataBoundary();
			if(DP.containsKey(GlobalParam.READER_LAST_STAMP))
				this.READER_LAST_STAMP = String.valueOf(DP.get(GlobalParam.READER_LAST_STAMP));
			if(DP.containsKey(GlobalParam.READER_STATUS))
				this.status = (boolean) DP.get(GlobalParam.READER_STATUS);
			this.datas = (LinkedList<PipeDataUnit>) DP.getData();
		}
	}
 
	public String getIncrementColumn() {
		return IncrementColumn;
	}
 
	public PipeDataUnit getLineData() {  
		return this.datas.poll();
	}
 
	public boolean nextLine() {
		if (datas.isEmpty()) {
			return false; 
		}
		return true;
	}
 
	public void close() {
		this.READER_LAST_STAMP = "";
		this.dataBoundary = null;
		this.status = true;
		this.keyColumn = null;
		this.IncrementColumn = null;
	}
 
	public String getScanStamp() {
		return READER_LAST_STAMP;
	}
  
	public String getDataBoundary() {
		return dataBoundary;
	}
 
	public String getkeyColumn() { 
		return keyColumn;
	}
 
	public boolean status() { 
		return status;
	} 
}
