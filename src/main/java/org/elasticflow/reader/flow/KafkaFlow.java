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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:24
 */
@NotThreadSafe
public class KafkaFlow extends ReaderFlowSocket {
	
	final int readms = 2000;
	ConsumerRecords<String, String> records;

	private final static Logger log = LoggerFactory.getLogger(KafkaFlow.class);

	public static KafkaFlow getInstance(final ConnectParams connectParams) {
		KafkaFlow o = new KafkaFlow();
		o.INIT(connectParams);
		return o;
	}

	@Override
	public DataPage getPageData(final Page page, int pageSize) {
		if (this.records == null) {
			return this.dataPage;
		}
		int count = 0;
		boolean releaseConn = false;
		PREPARE(false, false);
		@SuppressWarnings("unchecked")
		KafkaConsumer<String, String> conn = (KafkaConsumer<String, String>) GETSOCKET().getConnection(END_TYPE.reader);
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
			
			conn.commitSync();
		} catch (Exception e) {
			releaseConn = true;
			this.dataPage.put(GlobalParam.READER_STATUS, false);
			log.error("get Page Data Exception so free connection,details ", e);
		} finally {
			REALEASE(false, releaseConn);
		}
		return this.dataPage;
	}

	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(final Task task, int pageSize) {
		boolean releaseConn = false;
		PREPARE(false, false);
		@SuppressWarnings("unchecked")
		KafkaConsumer<String, String> conn = (KafkaConsumer<String, String>) GETSOCKET().getConnection(END_TYPE.reader);
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();
		try {
			while (true) {
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
					break;
				}
			}

		} catch (Exception e) {
			releaseConn = true;
			page.clear();
			log.error("get dataPage Exception so free connection,details ", e);
		} finally {
			REALEASE(false, releaseConn);
		}
		return page;
	}

}