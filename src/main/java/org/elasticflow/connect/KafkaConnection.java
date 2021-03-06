package org.elasticflow.connect;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticflow.config.GlobalParam;
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
public class KafkaConnection extends EFConnectionSocket<KafkaConsumer<String, String>> {

	private KafkaConsumer<String, String> conn = null;

	private final static Logger log = LoggerFactory.getLogger("Kafka Socket");
	
	private final int MAX_FETCH_BYTES = 1048576 * 10; //1M * n

	public static EFConnectionSocket<?> getInstance(ConnectParams ConnectParams) {
		EFConnectionSocket<?> o = new KafkaConnection();
		o.init(ConnectParams);
		o.connect();
		return o;
	}

	@Override
	public boolean connect() {
		WarehouseNosqlParam wnp = (WarehouseNosqlParam) this.connectParams.getWhp();
		if (wnp.getPath() != null) {
			if (!status()) { 
				Properties props = new Properties();
				String[] tmps = wnp.getDefaultValue().split("#");
		        props.put("bootstrap.servers", wnp.getPath());		        
		        props.put("group.id", tmps[0]);
		        props.put("key.deserializer", StringDeserializer.class);
		        props.put("value.deserializer", StringDeserializer.class);
		        props.put("max.poll.records",GlobalParam.READ_PAGE_SIZE);
		        props.put("max.partition.fetch.bytes", MAX_FETCH_BYTES);
		        if(tmps.length!=2) {
		        	log.error("kafka group.id and topic setting wrong!");
		        	return false;
		        }		        	
				this.conn = new KafkaConsumer<String, String>(props);
				this.conn.subscribe(Arrays.asList(tmps[1].split(",")));
			}
		} else {
			return false;
		}
		return true;
	}

	@Override
	public KafkaConsumer<String, String> getConnection(boolean searcher) {
		int tryTime = 0;
		try {
			while (tryTime < 5 && !connect()) {
				tryTime++;
				Thread.sleep(2000);
			}
		} catch (Exception e) {
			log.error("try to get Connection Exception,", e);
		}
		return this.conn;
	}

	@Override
	public boolean status() {
		if (this.conn == null) {
			return false;
		}
		return true;
	}

	@Override
	public boolean free() {
		try {
			this.conn.close();
			this.conn = null;
			this.connectParams = null;
		} catch (Exception e) {
			log.error("free connect Exception,", e);
			return false;
		}
		return true;
	}

}
