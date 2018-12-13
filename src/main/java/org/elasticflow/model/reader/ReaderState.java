package org.elasticflow.model.reader;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class ReaderState { 
	private String ReaderScanStamp = "0";
	private int count = 0;
	boolean status = true;
	
 
	public String getReaderScanStamp() {
		return ReaderScanStamp;
	}
	public void setReaderScanStamp(String ReaderScanStamp) {
		this.ReaderScanStamp = ReaderScanStamp;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public boolean isStatus() {
		return status;
	}
	public void setStatus(boolean status) {
		this.status = status;
	} 
}
