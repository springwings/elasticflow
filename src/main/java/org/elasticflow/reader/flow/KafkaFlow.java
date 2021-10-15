package org.elasticflow.reader.flow;

import java.time.Duration;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.model.Page;
import org.elasticflow.model.Task;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * The message is confirmed manually 
 * when autoCommit is false.
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:24
 */
@NotThreadSafe
public class KafkaFlow extends ReaderFlowSocket {
	
	private int readms = 3000;
	
	private boolean autoCommit = false;
	  
	ConsumerRecords<String, String> records;
	
	private int noDataTimes = 0;

	private final static Logger log = LoggerFactory.getLogger(KafkaFlow.class);
	
	private KafkaConsumer<String, String> conn = null;
	
	
	public static KafkaFlow getInstance(final ConnectParams connectParams) {
		KafkaFlow o = new KafkaFlow();
		o.INIT(connectParams);
		if(connectParams.getWhp().getCustomParams()!=null) {
			if(connectParams.getWhp().getCustomParams().containsKey("max.poll.interval.ms"))
				o.readms = connectParams.getWhp().getCustomParams().getIntValue("max.poll.interval.ms");
			if(connectParams.getWhp().getCustomParams().containsKey("enable.auto.commit")) {
				String tmp = connectParams.getWhp().getCustomParams().getString("enable.auto.commit");
				if(tmp.toLowerCase().equals("true")) {
					o.autoCommit = true;
				}
			} 
		}  
		o.initconn();
		return o;
	}
	
	@SuppressWarnings("unchecked")
	private void initconn() {
		PREPARE(true, false);
		this.conn = (KafkaConsumer<String, String>) GETSOCKET().getConnection(END_TYPE.reader);
	}

	@Override
	public DataPage getPageData(final Page page, int pageSize) {
		if (this.records == null) {
			return this.dataPage;
		}
		int count = 0;
		boolean releaseConn = false;
		try {
			String dataBoundary = null;
			String LAST_STAMP = null;
			this.dataPage.put(GlobalParam.READER_KEY, page.getReaderKey());
			this.dataPage.put(GlobalParam.READER_SCAN_KEY, page.getReaderScanKey());
			if(this.readHandler==null){
				for (ConsumerRecord<String, String> record : this.records) {
					count++;
					if (count >= Integer.valueOf(page.getStart()) && count < Integer.valueOf(page.getEnd())) {				
						PipeDataUnit u = PipeDataUnit.getInstance();
						String val = record.value();
						JSONObject jsonObject = JSON.parseObject(val);
						Set<Entry<String, Object>> itr = jsonObject.entrySet();	
						for (Entry<String, Object> k : itr) {
							PipeDataUnit.addFieldValue(k.getKey(), k.getValue(), page.getInstanceConfig().getReadFields(),u);
							if (k.getKey().equals(this.dataPage.get(GlobalParam.READER_SCAN_KEY))) {
								LAST_STAMP = String.valueOf(k.getValue());
							}else if(k.getKey().equals(this.dataPage.get(GlobalParam.READER_KEY))) {
								u.setReaderKeyVal(k.getValue());
								dataBoundary = String.valueOf(k.getValue());
							}						
						}
						this.dataUnit.add(u);
					}
				}
				if (LAST_STAMP == null) {
					this.dataPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis());
				} else {
					this.dataPage.put(GlobalParam.READER_LAST_STAMP, LAST_STAMP);
				} 
				this.dataPage.putData(this.dataUnit);
				this.dataPage.putDataBoundary(dataBoundary);		
			}else {
				//handler reference mysql flow getAllData function 
				this.readHandler.handleData(this,this.records,page,pageSize);
			}    
			
		} catch (Exception e) {
			releaseConn = true;
			this.dataPage.put(GlobalParam.READER_STATUS, false);
			REALEASE(false, releaseConn);
			this.initconn();
			log.error("get Page Data Exception so free connection,details ", e);
		}  
		return this.dataPage;
	}
	
	/**
	 * Do not release the connection unless there is a processing error
	 * @throws EFException 
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void flush() throws EFException { 
		if(!this.autoCommit) { 
			try {
				((KafkaConsumer<String, String>) GETSOCKET().getConnection(END_TYPE.reader)).commitSync();
			} catch (Exception e) {
				throw new EFException(e, ELEVEL.Termination);
			}
		}	
	}
	
	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(final Task task, int pageSize) {
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();	
		try {
			this.records = conn.poll(Duration.ofMillis(readms));
			int totalNum = this.records.count();
			if (totalNum > 0) {
				int pagenum = (int) Math.ceil(totalNum / pageSize);
				int curentpage = 0;
				while (true) {
					curentpage++;
					page.add(String.valueOf(curentpage * pageSize));
					if (curentpage >= pagenum)
						break;
				}
				noDataTimes = 0;
			}else {
				noDataTimes +=1;
			}
		} catch (Exception e) {
			page.clear();
			REALEASE(false, true);
			this.initconn();
			log.error("Error in getting Kafka data, the connection will be cleared automatically.", e);
		} 
		if(noDataTimes>5)
			REALEASE(false, true);
		return page;
	}
}