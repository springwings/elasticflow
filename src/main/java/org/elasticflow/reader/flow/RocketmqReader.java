package org.elasticflow.reader.flow;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
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
import org.elasticflow.util.EFException.ETYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * RocketMQReader Running in fixed single connection mode. The message format
 * must be in JSON format, and the key must correspond one-to-one with the
 * fields defined in the piper when autoCommit is false.
 * 
 * NotThreadSafe When starting multi-channel concurrency, careful consideration
 * should be given to whether bugs will be introduced
 * 
 * @author chengwen
 * @version 1.0
 * @date 2023-05-04 09:24
 */

@NotThreadSafe
public class RocketmqReader extends ReaderFlowSocket {

	private boolean autoCommit = false;

	private final static Logger log = LoggerFactory.getLogger(RocketmqReader.class);

	private DefaultLitePullConsumer conn = null;

	private List<MessageExt> records;

	public static RocketmqReader getInstance(final ConnectParams connectParams) {
		RocketmqReader o = new RocketmqReader();
		o.initConn(connectParams);
		if (connectParams.getWhp().getCustomParams() != null) {
			if (connectParams.getWhp().getCustomParams().containsKey("AutoCommit")) {
				o.autoCommit = connectParams.getWhp().getCustomParams().getBooleanValue("AutoCommit");
			}
		}
		return o;
	}

	@Override
	public void initFlow() throws EFException {
		PREPARE(true, false, true);
		this.conn = (DefaultLitePullConsumer) GETSOCKET().getConnection(END_TYPE.reader);
		try {
			this.conn.start();
		} catch (MQClientException e) {
			log.error("RocketMQ start Exception", e);
		}
	}

	@Override
	public DataPage getPageData(final Page page, int pageSize) throws EFException {
		if (this.records == null) {
			return this.dataPage;
		}
		int count = 0;
		String dataBoundary = null;
		String LAST_STAMP = null;
		this.dataPage.put(GlobalParam.READER_KEY, page.getReaderKey());
		this.dataPage.put(GlobalParam.READER_SCAN_KEY, page.getReaderScanKey());
		try {
			if (this.readHandler == null) {
				for (MessageExt record : this.records) {
					count++;
					if (count >= Integer.valueOf(page.getStart()) && count < Integer.valueOf(page.getEnd())) {
						PipeDataUnit u = PipeDataUnit.getInstance();
						String val = new String(record.getBody(), "utf-8");
						JSONObject jsonObject = JSON.parseObject(val);
						Set<Entry<String, Object>> itr = jsonObject.entrySet();
						for (Entry<String, Object> k : itr) {
							PipeDataUnit.addFieldValue(k.getKey(), k.getValue(),
									page.getInstanceConfig().getReadFields(), u);
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
				this.readHandler.handleData(this, this.records, page, pageSize);
			} 
		} catch (Exception e) {
			this.dataPage.put(GlobalParam.READER_STATUS, false);
			log.error("RocketMQ Reader get dataPage Exception!", e);
			throw new EFException("RocketMQ Reader get dataPage Exception!");
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
	public ConcurrentLinkedDeque<String> getPageSplit(final Task task, int pageSize) throws EFException {
		boolean releaseConn = false;
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();
		try {
			this.records = this.conn.poll();
			int totalNum = this.records.size();
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
			REALEASE(true, releaseConn);
			try {
				initFlow();
			} catch (EFException e1) {
				throw e1;
			}
			log.error("{} RocketMQ Reader get page lists Exception, system will auto free connection!",
					task.getInstanceID(), e);
			throw new EFException("RocketMQ Reader get page lists Exception!");
		}
		return page;
	}

}