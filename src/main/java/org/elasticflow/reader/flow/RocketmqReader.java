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

	private DefaultLitePullConsumer conn = null;

	private List<MessageExt> records;
	
	public static boolean crossSubtasks = true;
	
	public boolean isDiffEndType = false;

	public static RocketmqReader getInstance(final ConnectParams connectParams) {
		RocketmqReader o = new RocketmqReader();
		o.initConn(connectParams);
		if (connectParams.getWhp().getCustomParams() != null && connectParams.getWhp().getCustomParams().containsKey("AutoCommit")) {
			o.autoCommit = connectParams.getWhp().getCustomParams().getBooleanValue("AutoCommit"); 
		}
		return o;
	}

	@Override
	public void initFlow() throws EFException {
		this.isConnMonopoly = true;
		PREPARE(true, false);
		this.conn = (DefaultLitePullConsumer) GETSOCKET().getConnection(END_TYPE.reader);
		try {
			this.conn.start();
		} catch (MQClientException e) { 
			throw new EFException(e,"RocketMQ start Exception!");
		}
	}

	@Override
	public DataPage getPageData(final TaskCursor taskCursor, int pageSize) throws EFException {
		if (this.records == null) {
			return this.dataPage;
		}
		int count = 0;
		String dataBoundary = null;
		String LAST_STAMP = null;
		this.dataPage.put(GlobalParam.READER_KEY, taskCursor.getReaderKey());
		this.dataPage.put(GlobalParam.READER_SCAN_KEY, taskCursor.getReaderScanKey());
		try {
			if (this.readHandler == null) {
				for (MessageExt record : this.records) {
					count++;
					if (count >= Integer.valueOf(taskCursor.getStart()) && count < Integer.valueOf(taskCursor.getEnd())) {
						PipeDataUnit u = PipeDataUnit.getInstance();
						String val = new String(record.getBody(), "utf-8");
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
				this.readHandler.handleData(this, this.records, taskCursor, pageSize);
			} 
		} catch (Exception e) {
			this.dataPage.put(GlobalParam.READER_STATUS, false); 
			throw new EFException(e,taskCursor.getInstanceConfig().getInstanceID()+ " RocketMQ Reader get dataPage Exception!");
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
				throw new EFException(e, "RocketMQ Reader flush Exception");
			}
		}
	}

	@Override
	public ConcurrentLinkedDeque<String> getDataPages(final TaskModel task, int pageSize) throws EFException { 
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
				if(this.autoCommit==false)
					this.setCached(true);
			}
		} catch (Exception e) { 
			page.clear();
			releaseConn(true, true);
			try {
				initFlow();
			} catch (EFException e1) {
				throw e1;
			} 
			throw new EFException(e,task.getInstanceID()+ " RocketMQ Reader get page lists Exception!");
		}
		return page;
	}

}