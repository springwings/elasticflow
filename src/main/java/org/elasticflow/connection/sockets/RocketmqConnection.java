package org.elasticflow.connection.sockets;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.connection.EFConnectionSocket;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocketMQ basic connection establishment management class
 * @author chengwen
 * @version 1.0
 * @date 2021-06-24 09:25
 * @see https://rocketmq.apache.org/
 */
public class RocketmqConnection extends EFConnectionSocket<Object> {

	private DefaultLitePullConsumer cconn = null; 
	
	private DefaultMQProducer pconn = null;
	
	final String CUSTOM_CONSUMER_TOPIC = "consumer.topic";	
	
	final String CONSUMER_GROUP_ID = "group.id";
	
	final String PRODUCER_GROUP_ID = "producer.group.id";

	private final static Logger log = LoggerFactory.getLogger("Kafka Socket");

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new RocketmqConnection();
		o.init(ConnectParams);
		o.setWriteShare(false);
		o.setReadShare(false);
		return o;
	}

	@Override
	public boolean connect(END_TYPE endType) {
		WarehouseParam wnp = this.connectParams.getWhp();
		if (wnp.getHost() != null) {
			if (!status()) {
				try {
					if(endType==END_TYPE.reader) 
						this.genConsumer(wnp);
					else if(endType==END_TYPE.writer)
						this.genProducer(wnp);
				} catch (Exception e) {
					log.error("{} Rocketmq {} connect exception", this.connectParams.getWhp().getAlias(),endType.name(),e);
				}			
			}
		} else {
			return false;
		}
		return true;
	}
	
	/**
	 * topic setting     topic:tag,topic:tag
	 * @param wnp
	 * @throws MQClientException 
	 */
	private void genConsumer(WarehouseParam wnp) throws MQClientException { 
		this.cconn = new DefaultLitePullConsumer(wnp.getDefaultValue().getString(CONSUMER_GROUP_ID));
		this.cconn.setNamesrvAddr(wnp.getHost());
		this.cconn.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET); 
        String[] topicArr = wnp.getDefaultValue().getString(CUSTOM_CONSUMER_TOPIC).split(",");
        for (String tag : topicArr) {
            String[] tagArr = tag.split(":");
            this.cconn.subscribe(tagArr[0], tagArr[1]);
        } 
        this.cconn.setAutoCommit(false);
        if(wnp.getCustomParams()!=null) {
        	if(wnp.getCustomParams().containsKey("PollTimeoutMillis"))
        		this.cconn.setPollTimeoutMillis(wnp.getCustomParams().getIntValue("PollTimeoutMillis"));
        	if(wnp.getCustomParams().containsKey("PullBatchSize"))
        		this.cconn.setPullBatchSize(wnp.getCustomParams().getIntValue("PullBatchSize"));
        	if(wnp.getCustomParams().containsKey("PullThreadNums"))
        		this.cconn.setPullThreadNums(wnp.getCustomParams().getIntValue("ConsumeThreadMin"));
        	if(wnp.getCustomParams().containsKey("AutoCommit"))
        		this.cconn.setAutoCommit(wnp.getCustomParams().getBooleanValue("AutoCommit"));
        } 
	}
	
	private void genProducer(WarehouseParam wnp) {
		this.pconn = new
	            DefaultMQProducer(wnp.getDefaultValue().getString(PRODUCER_GROUP_ID));
		this.pconn.setNamesrvAddr(wnp.getHost()); 
		if(wnp.getCustomParams()!=null) { 
			if(wnp.getCustomParams().containsKey("VipChannelEnabled"))
        		this.pconn.setVipChannelEnabled(wnp.getCustomParams().getBoolean("VipChannelEnabled"));
			if(wnp.getCustomParams().containsKey("MaxMessageSize"))
        		this.pconn.setMaxMessageSize(wnp.getCustomParams().getIntValue("MaxMessageSize"));
			if(wnp.getCustomParams().containsKey("SendMsgTimeout"))
        		this.pconn.setSendMsgTimeout(wnp.getCustomParams().getIntValue("SendMsgTimeout"));
			if(wnp.getCustomParams().containsKey("RetryTimesWhenSendAsyncFailed"))
        		this.pconn.setRetryTimesWhenSendAsyncFailed(wnp.getCustomParams().getIntValue("RetryTimesWhenSendAsyncFailed")); 
        }
	}

	@Override
	public Object getConnection(END_TYPE endType) {
		this.endType = endType;
		int tryTime = 0;
		try {
			while (tryTime < 5 && !connect(endType)) {
				tryTime++;
				Thread.sleep(1000+tryTime*500);
			}
		} catch (Exception e) {
			log.error("{} get Rocketmq {} connection exception", this.connectParams.getWhp().getAlias(),endType.name(),e);
		}
		if(endType==END_TYPE.reader) {
			return this.cconn;
		}else {
			return this.pconn;
		}
	}

	@Override
	public boolean status() {
		if(this.endType==END_TYPE.reader && this.cconn == null)
			return false;
		if (this.endType==END_TYPE.writer && this.pconn == null) {
			return false;
		}
		return true;
	}

	@Override
	public boolean free() {
		try {
			if(this.cconn!=null) {
				this.cconn.shutdown();
				this.cconn = null;
			}
			if(this.pconn!=null) {
				this.pconn.shutdown();
				this.pconn = null;
			}
			this.connectParams = null;
		} catch (Exception e) {
			log.warn("{} free Rocketmq connection exception", this.connectParams.getWhp().getAlias(),e);
			return false;
		}
		return true;
	}

}
