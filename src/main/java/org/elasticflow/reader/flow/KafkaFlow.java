package org.elasticflow.reader.flow;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticflow.model.Page;
import org.elasticflow.model.Task;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.ReaderFlowSocket;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:24
 */
public class KafkaFlow extends ReaderFlowSocket{    
	
	ConsumerRecords<String, String> records;
	
	@SuppressWarnings("unchecked")
	KafkaConsumer<String,String> conn = (KafkaConsumer<String, String>) GETSOCKET().getConnection(false); 

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
		for (ConsumerRecord<String, String> record : records) {
            PipeDataUnit u = PipeDataUnit.getInstance();
            u.addFieldValue(record.key(), record.value(), page.getTransField());
            this.dataUnit.add(u);
            count++;
            this.conn.commitAsync();
            if(count>=pageSize)
            	break;
        }
		return this.dataPage;
	} 
	
	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(final Task task,int pageSize) {
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();		
		this.records = this.conn.poll(Duration.ofMillis(100)); 
		int num = (int) Math.ceil(this.records.count()/pageSize);
		while(num>0) {
			page.add(String.valueOf(num));
			num--;
		}
		return page;
	} 
	 
}