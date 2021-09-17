package org.elasticflow.reader;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticflow.flow.Flow;
import org.elasticflow.model.Page;
import org.elasticflow.model.Task;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.handler.ReadHandler;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-12 14:28
 */
@NotThreadSafe
public abstract class ReaderFlowSocket extends Flow{  
	
	/** defined custom read flow handler */
	protected ReadHandler readHandler;

	protected DataPage dataPage = new DataPage(); 
	
	protected ConcurrentLinkedQueue<PipeDataUnit> dataUnit = new ConcurrentLinkedQueue<>(); 
	
	public final Lock lock = new ReentrantLock(); 	
	
	
	@Override
	public void INIT(ConnectParams connectParams) {
		this.connectParams = connectParams; 
		this.poolName = connectParams.getWhp().getPoolName(connectParams.getL1Seq()); 		
	} 
	
	public void setReaderHandler(ReadHandler readHandler) {
		this.readHandler = readHandler;
	}
	
	public ReadHandler getReaderHandler() {
		return readHandler;
	}	
	
	public ConcurrentLinkedQueue<PipeDataUnit> getDataUnit(){
		return dataUnit;
	}
	
	public DataPage getDataPage() {
		return dataPage;
	}
	 
	public abstract DataPage getPageData(final Page page,int pageSize);

	public abstract ConcurrentLinkedDeque<String> getPageSplit(final Task task,int pageSize);
	
	/**
	 * Transaction confirmation
	 */
	public void flush() {
		
	}
	
	/**
	 * release job page
	 */
	public void freeJobPage() {
		this.dataPage.clear(); 
		this.dataUnit.clear();  
	} 
	
	
}
