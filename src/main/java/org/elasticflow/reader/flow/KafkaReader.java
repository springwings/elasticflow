/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
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
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.model.task.TaskCursor;
import org.elasticflow.model.task.TaskModel;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.util.EFException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * KafkaReader Running in fixed single connection mode The message format must
 * be in JSON format, and the key must correspond one-to-one with the fields
 * defined in the piper
 * 
 * NotThreadSafe 
 * When starting multi-channel concurrency, careful consideration should be given to whether bugs will be introduced
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:24
 */

@NotThreadSafe
public class KafkaReader extends ReaderFlowSocket {

	private int readms = 3000;

	private boolean autoCommit = false;

	ConsumerRecords<String, String> records;

	private KafkaConsumer<String, String> conn = null;
	
	public static boolean crossSubtasks = true;

	/**
	 * @param connectParams enable.auto.commit=false message is confirmed manually
	 * @return
	 */
	public static KafkaReader getInstance(final ConnectParams connectParams) {
		KafkaReader o = new KafkaReader();
		o.initConn(connectParams);
		if (connectParams.getWhp().getCustomParams() != null) {
			if (connectParams.getWhp().getCustomParams().containsKey("timeout.ms"))
				o.readms = connectParams.getWhp().getCustomParams().getIntValue("timeout.ms");
			if (connectParams.getWhp().getCustomParams().containsKey("enable.auto.commit")) {
				o.autoCommit = connectParams.getWhp().getCustomParams().getBooleanValue("enable.auto.commit");
			}
		}
		return o;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void initFlow() throws EFException {
		this.isConnMonopoly = true;
		this.isDiffEndType = true;
		PREPARE(true, false);
		this.conn = (KafkaConsumer<String, String>) GETSOCKET().getConnection(END_TYPE.reader);
	}

	@Override
	public DataPage getPageData(final TaskCursor taskCursor, int pageSize) throws EFException {
		if (this.records == null) {
			return this.dataPage;
		}
		int count = 0; 
		try {
			String dataBoundary = null;
			String LAST_STAMP = null;
			this.dataPage.put(GlobalParam.READER_KEY, taskCursor.getReaderKey());
			this.dataPage.put(GlobalParam.READER_SCAN_KEY, taskCursor.getReaderScanKey());
			if (this.readHandler == null && this.readHandler.supportHandleData()) {
				for (ConsumerRecord<String, String> record : this.records) {
					count++;
					if (count >= Integer.valueOf(taskCursor.getStart()) && count < Integer.valueOf(taskCursor.getEnd())) {
						PipeDataUnit u = PipeDataUnit.getInstance();
						String val = record.value();
						JSONObject jsonObject = JSON.parseObject(val);
						Set<Entry<String, Object>> itr = jsonObject.entrySet();
						for (Entry<String, Object> k : itr) {
							PipeDataUnit.addFieldValue(k.getKey(), k.getValue(),
									taskCursor.getInstanceConfig().getReadFields(), u);
							if (k.getKey().equals(this.dataPage.get(GlobalParam.READER_SCAN_KEY))) {
								LAST_STAMP = String.valueOf(k.getValue());
							} else if (k.getKey().equals(this.dataPage.get(GlobalParam.READER_KEY))) {
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
			} else {
				// handler reference mysql flow getAllData function
				this.readHandler.handleData(this, this.records, taskCursor, pageSize);
			}

		} catch (Exception e) { 
			this.dataPage.put(GlobalParam.READER_STATUS, false);  
			throw new EFException(e,taskCursor.getInstanceConfig().getInstanceID()+ " Kafka Reader get dataPage Exception");
		}
		return this.dataPage;
	}

	/**
	 * Do not release the connection unless there is a processing error
	 * 
	 * @throws EFException
	 */
	@Override
	public void flush() throws EFException {
		if (!this.autoCommit) {
			try {
				conn.commitSync();
			} catch (Exception e) {
				releaseConn(false, true);
				try {
					this.initFlow();
				} catch (EFException e1) {
					throw e1;
				}   
				throw new EFException(e,"kafka data flush exception!");
			}
		}
	}

	@Override
	public ConcurrentLinkedDeque<String> getDataPages(final TaskModel task, int pageSize) throws EFException { 
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();
		try {  
			this.records = conn.poll(Duration.ofMillis(readms));
			int totalNum = this.records.count();
			if (totalNum > 0) {
				int pagenum = (int) Math.ceil((totalNum + 0.) / pageSize);
				int curentpage = 0;
				while (true) {
					curentpage++;
					page.add(String.valueOf(curentpage * pageSize));
					if (curentpage >= pagenum)
						break;
				}
			}
		} catch (Exception e) { 
			page.clear();
			releaseConn(false, true);
			try {
				this.initFlow();
			} catch (EFException e1) {
				throw e1;
			}   
			throw new EFException(e,task.getInstanceID()+ " Kafka Reader get page lists Exception!");
		}
		return page;
	}

}