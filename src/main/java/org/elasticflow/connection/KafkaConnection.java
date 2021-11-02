package org.elasticflow.connection;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseNosqlParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
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
	
	private final int MAX_FETCH_BYTES = 1048576 * 10; //1M * n

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new KafkaConnection();
		o.init(ConnectParams);
		return o;
	}

	@Override
	protected boolean connect(END_TYPE endType) {
		WarehouseNosqlParam wnp = (WarehouseNosqlParam) this.connectParams.getWhp();
		if (wnp.getPath() != null) {
			if (!status()) { 			
				if(endType.equals(END_TYPE.reader))
					this.genConsumer(wnp);
				if(endType.equals(END_TYPE.writer))
					this.genProducer(wnp);
			}
		} else {
			return false;
		}
		return true;
	}
	
	private void genConsumer(WarehouseNosqlParam wnp) {
		Properties props = new Properties();
        props.put("bootstrap.servers", wnp.getPath());		        
        props.put("group.id",wnp.getDefaultValue().getString(CUSTOM_GROUP_ID));
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        props.put("max.poll.records",GlobalParam.READ_PAGE_SIZE);
        props.put("max.partition.fetch.bytes", MAX_FETCH_BYTES);
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "15000");
        props.put("heartbeat.interval.ms", "3000");
        if(wnp.getCustomParams()!=null) {
        	if(wnp.getCustomParams().containsKey("max.poll.records"))
        		props.put("max.poll.records",wnp.getCustomParams().getString("max.poll.records"));
        	if(wnp.getCustomParams().containsKey("max.partition.fetch.bytes"))
        		props.put("max.partition.fetch.bytes",wnp.getCustomParams().getString("max.partition.fetch.bytes"));
        	if(wnp.getCustomParams().containsKey("session.timeout.ms"))
        		props.put("session.timeout.ms",wnp.getCustomParams().getString("session.timeout.ms"));
        	if(wnp.getCustomParams().containsKey("heartbeat.interval.ms"))
        		props.put("heartbeat.interval.ms",wnp.getCustomParams().getString("heartbeat.interval.ms"));
        	if(wnp.getCustomParams().containsKey("enable.auto.commit"))
        		props.put("enable.auto.commit",wnp.getCustomParams().getString("enable.auto.commit"));
        }
        this.cconn = new KafkaConsumer<String, String>(props);
		this.cconn.subscribe(Arrays.asList(wnp.getDefaultValue().getString(CUSTOM_CONSUMER_TOPIC).split(",")));
	}
	
	private void genProducer(WarehouseNosqlParam wnp) {
		Properties props = new Properties();
        props.put("bootstrap.servers", wnp.getPath());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",StringSerializer.class);
        props.put("value.serializer",StringSerializer.class);
        if(wnp.getCustomParams()!=null) {
        	if(wnp.getCustomParams().containsKey("batch.size"))
        		props.put("batch.size",wnp.getCustomParams().getString("batch.size"));
        	if(wnp.getCustomParams().containsKey("acks"))
        		props.put("acks",wnp.getCustomParams().getString("acks"));
        }
        this.pconn = new KafkaProducer<>(props);
	}

	@Override
	public Object getConnection(END_TYPE endType) {
		int tryTime = 0;
		try {
			while (tryTime < 5 && !connect(endType)) {
				tryTime++;
				Thread.sleep(2000);
			}
		} catch (Exception e) {
			log.error("try to get Connection Exception,", e);
		}
		if(endType==END_TYPE.reader) {
			return this.cconn;
		}else {
			return this.pconn;
		}
	}

	@Override
	public boolean status() {
		if (this.cconn == null) {
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
			log.error("free connect Exception,", e);
			return false;
		}
		return true;
	}

}
