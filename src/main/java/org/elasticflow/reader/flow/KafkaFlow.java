package org.elasticflow.reader.flow;

import java.time.Duration;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

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
@NotThreadSafe
public class KafkaFlow extends ReaderFlowSocket {
	
	final int readms = 100;
	ConsumerRecords<String, String> records;
	int totalNum = 0;
	AtomicInteger counts = new AtomicInteger(0);

	private final static Logger log = LoggerFactory.getLogger(KafkaFlow.class);

	public static KafkaFlow getInstance(final ConnectParams connectParams) {
		KafkaFlow o = new KafkaFlow();
		o.INIT(connectParams);
		return o;
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataPage getPageData(final Page page, int pageSize) {

		if (this.records == null || this.totalNum <= this.counts.get()) {
			this.totalNum = 0;
			((KafkaConsumer<String, String>) GETSOCKET().getConnection(false)).commitAsync();
			return this.dataPage;
		}

		int count = 0;
		boolean releaseConn = false;
		PREPARE(false, false);
		try {
			String dataBoundary = null;
			String LAST_STAMP = null;
			for (ConsumerRecord<String, String> record : records) {
				count++;
				if (count >= Integer.valueOf(page.getStart()) && count < Integer.valueOf(page.getEnd())) {
					PipeDataUnit u = PipeDataUnit.getInstance();
					String val = record.value();
					String key = record.key();
					JSONObject jsonObject = JSON.parseObject(val);
					Set<Entry<String, Object>> itr = jsonObject.entrySet();
					u.setReaderKeyVal(key);
					dataBoundary = key;
					for (Entry<String, Object> k : itr) {
						u.addFieldValue(k.getKey(), k.getValue(), page.getTransField());
						if (k.getKey().equals(this.dataPage.get(GlobalParam.READER_SCAN_KEY))) {
							LAST_STAMP = String.valueOf(k.getValue());
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
			this.dataPage.put(GlobalParam.READER_KEY, page.getReaderKey());
			this.dataPage.put(GlobalParam.READER_SCAN_KEY, page.getReaderScanKey());
			this.dataPage.putData(this.dataUnit);
			this.dataPage.putDataBoundary(dataBoundary);
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
		KafkaConsumer<String, String> conn = (KafkaConsumer<String, String>) GETSOCKET().getConnection(false);
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();
		try {
			while (true) {
				this.records = conn.poll(Duration.ofMillis(readms));
				this.totalNum = this.records.count();
				if (this.totalNum > 0) {
					int pagenum = (int) Math.ceil(this.totalNum / pageSize);
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