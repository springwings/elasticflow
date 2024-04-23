package org.elasticflow.connection.sockets;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.connection.EFConnectionSocket;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka basic connection establishment management class
 * @author chengwen
 * @version 1.0
 * @date 2021-06-24 09:25
 */
public class KafkaConnection extends EFConnectionSocket<Object> {

	private KafkaConsumer<String, String> cconn = null;
	
	private KafkaProducer<String, String> pconn = null;
	
	final String CUSTOM_CONSUMER_TOPIC = "consumer.topic";	
	
	final String CUSTOM_GROUP_ID = "group.id";

	private final static Logger log = LoggerFactory.getLogger("Kafka Socket");
	
	private final int MAX_FETCH_BYTES = 1048576 * 4; //1M * n
	
	private final int MAX_REQUEST_SIZE = 1048576; //1M * n

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new KafkaConnection();
		o.init(ConnectParams);
		return o;
	}

	@Override
	public boolean connect(END_TYPE endType) { 
		WarehouseParam wnp = this.connectParams.getWhp();
		if (wnp.getHost() != null) {
			if (!status()) { 		 
				if(endType==END_TYPE.reader)
					this.genConsumer(wnp);
				else if(endType==END_TYPE.writer)
					this.genProducer(wnp);
			}
		} else {
			return false;
		}
		return true;
	}
	
	private void genConsumer(WarehouseParam wnp) {
		Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, wnp.getHost());		        
        props.put("group.id",wnp.getDefaultValue().getString(CUSTOM_GROUP_ID));
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,GlobalParam.READ_PAGE_SIZE);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, MAX_FETCH_BYTES);
        if(wnp.getCustomParams()!=null) {
        	if(wnp.getCustomParams().containsKey("max.poll.records"))
        		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,wnp.getCustomParams().getString("max.poll.records"));
        	if(wnp.getCustomParams().containsKey("fetch.max.bytes"))
        		props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,wnp.getCustomParams().getString("fetch.max.bytes"));
        	if(wnp.getCustomParams().containsKey("max.partition.fetch.bytes"))
        		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,wnp.getCustomParams().getString("max.partition.fetch.bytes"));
        	if(wnp.getCustomParams().containsKey("session.timeout.ms"))
        		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,wnp.getCustomParams().getString("session.timeout.ms"));
        	if(wnp.getCustomParams().containsKey("heartbeat.interval.ms"))
        		props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,wnp.getCustomParams().getString("heartbeat.interval.ms"));
        	if(wnp.getCustomParams().containsKey("enable.auto.commit"))
        		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,wnp.getCustomParams().getString("enable.auto.commit"));
        	if(wnp.getCustomParams().containsKey("request.timeout.ms"))
        		props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,wnp.getCustomParams().getString("request.timeout.ms"));
        }
        this.cconn = new KafkaConsumer<String, String>(props);
		this.cconn.subscribe(Arrays.asList(wnp.getDefaultValue().getString(CUSTOM_CONSUMER_TOPIC).split(",")));
	}
	
	private void genProducer(WarehouseParam wnp) {
		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, wnp.getHost());
        props.put(ProducerConfig.ACKS_CONFIG, "all"); 
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "90000");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, MAX_REQUEST_SIZE/8);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, MAX_REQUEST_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put("key.serializer",StringSerializer.class);
        props.put("value.serializer",StringSerializer.class);
        if(wnp.getCustomParams()!=null) {
        	if(wnp.getCustomParams().containsKey("batch.size"))
        		props.put(ProducerConfig.BATCH_SIZE_CONFIG,wnp.getCustomParams().getString("batch.size"));
        	if(wnp.getCustomParams().containsKey("max.request.size"))
        		props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,wnp.getCustomParams().getString("max.request.size")); 
        	if(wnp.getCustomParams().containsKey("request.timeout.ms"))
        		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,wnp.getCustomParams().getString("request.timeout.ms")); 
        	if(wnp.getCustomParams().containsKey("buffer.memory"))
        		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,wnp.getCustomParams().getString("buffer.memory"));
        	if(wnp.getCustomParams().containsKey("acks"))
        		props.put(ProducerConfig.ACKS_CONFIG,wnp.getCustomParams().getString("acks"));
        }
        this.pconn = new KafkaProducer<>(props);
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
			log.error("{} get kafka {} connection exception",this.connectParams.getWhp().getAlias(),endType.name(), e);
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
				this.cconn.close();
				this.cconn = null;
			}
			if(this.pconn!=null) {
				this.pconn.close();
				this.pconn = null;
			}
			this.connectParams = null;
		} catch (Exception e) {
			log.warn("{} free kafka connection exception", this.connectParams.getWhp().getAlias(),e.getMessage());
			return false;
		}
		return true;
	}

}
