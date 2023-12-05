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
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.EFException.ETYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private final static Logger log = LoggerFactory.getLogger(KafkaReader.class);

	private KafkaConsumer<String, String> conn = null;

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
		PREPARE(true, false, true);
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
			log.error("Kafka Reader get dataPage Exception!", e); 
			this.dataPage.put(GlobalParam.READER_STATUS, false); 
			throw new EFException("Kafka Reader get dataPage Exception!");
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
				throw new EFException(e, ELEVEL.Dispose, ETYPE.RESOURCE_ERROR);
			}
		}
	}

	@Override
	public ConcurrentLinkedDeque<String> getDataPages(final TaskModel task, int pageSize) throws EFException {
		boolean releaseConn = false;
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
			releaseConn = true;
			page.clear();
			REALEASE(false, releaseConn);
			try {
				this.initFlow();
			} catch (EFException e1) {
				throw e1;
			}
			log.error("{} Kafka Reader get page lists Exception, system will auto free connection!",
					task.getInstanceID(), e);
			throw new EFException("Kafka Reader get page lists Exception!");
		}
		return page;
	}

}