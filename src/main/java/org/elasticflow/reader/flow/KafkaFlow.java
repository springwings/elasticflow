package org.elasticflow.reader.flow;

import java.time.Duration;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticflow.config.GlobalParam;
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
public class KafkaFlow extends ReaderFlowSocket{    
	
	ConsumerRecords<String, String> records;
	
	private final static Logger log = LoggerFactory.getLogger(KafkaFlow.class);  
 
	public static KafkaFlow getInstance(final ConnectParams connectParams) {
		KafkaFlow o = new KafkaFlow();
		o.INIT(connectParams);			
		return o;
	}  
 
	@Override
	public DataPage getPageData(final Page page,int pageSize) { 
		if(this.records ==null || this.records.count()==0)
			return this.dataPage;
		int count=0;
		boolean releaseConn = false;
		PREPARE(false,false); 
		@SuppressWarnings("unchecked")
		KafkaConsumer<String,String> conn = (KafkaConsumer<String, String>) GETSOCKET().getConnection(false); 
		try{
			String dataBoundary = null;
			for (ConsumerRecord<String, String> record : records) {
	            PipeDataUnit u = PipeDataUnit.getInstance();
	            String val = record.value();
				String key = record.key();
				JSONObject jsonObject = JSON.parseObject(val);
				Set<Entry<String, Object>> itr = jsonObject.entrySet();
				u.setReaderKeyVal(key);
				dataBoundary = key;
				for (Entry<String, Object> k : itr) {		 
					u.addFieldValue(k.getKey(), k.getValue(), page.getTransField());
				} 
	            this.dataUnit.add(u);
	            count++;
	            conn.commitAsync();
	            if(count>=pageSize)
	            	break;
	        }
			this.dataPage.putDataBoundary(dataBoundary);
		}catch (Exception e) {
			releaseConn = true;
			this.dataPage.put(GlobalParam.READER_STATUS,false);
			log.error("get Page Data Exception so free connection,details ", e);
		}finally{ 
			REALEASE(false,releaseConn);  
		}  
		return this.dataPage;
	} 
	
	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(final Task task,int pageSize) {
		boolean releaseConn = false;
		PREPARE(false,false); 
		@SuppressWarnings("unchecked")
		KafkaConsumer<String,String> conn = (KafkaConsumer<String, String>) GETSOCKET().getConnection(false);
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();		
		try{			
			this.records = conn.poll(Duration.ofMillis(100)); 
			int num = (int) Math.ceil(this.records.count()/pageSize);
			while(num>0) {
				page.add(String.valueOf(num));
				num--;
			}
		}catch (Exception e) {
			releaseConn = true;
			page.clear();
			log.error("get dataPage Exception so free connection,details ", e);
		}finally{ 
			REALEASE(false,releaseConn);  
		}  
		return page;
	} 
	 
}