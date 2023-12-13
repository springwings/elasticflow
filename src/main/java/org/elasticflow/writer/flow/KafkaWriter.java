package org.elasticflow.writer.flow;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.writer.WriterFlowSocket;
import org.elasticflow.yarn.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka flow Writer Manager
 * @author chengwen
 * @version 1.0 
 */

public class KafkaWriter extends WriterFlowSocket {
	
	private final static Logger log = LoggerFactory.getLogger(KafkaWriter.class);
	
	public static KafkaWriter getInstance(ConnectParams connectParams) {
		KafkaWriter o = new KafkaWriter();
		o.initConn(connectParams);
		return o;
	} 

	@SuppressWarnings("unchecked")
	private KafkaProducer<String, String> getconn() throws EFException {
		return (KafkaProducer<String, String>) GETSOCKET().getConnection(END_TYPE.writer);
	}
	
	@Override
	public void write(InstanceConfig instanceConfig,PipeDataUnit unit,String instance,
			String storeId, boolean isUpdate) throws EFException {
		if (!ISLINK())
			return; 
		Map<String, EFField> transParams = instanceConfig.getWriteFields();
		try { 
			for (Entry<String, Object> r : unit.getData().entrySet()) {
				String field = r.getKey();
				if (r.getValue() == null)
					continue;
				EFField transParam = transParams.get(field);
				if (transParam == null)
					transParam = transParams.get(field.toLowerCase());
				if (transParam == null)
					continue;
				if(transParam.getStored().equals("true")) {
					Object val = r.getValue();
					getconn().send(new ProducerRecord<String, String>(transParams.get("topic").getDefaultvalue(), unit.getReaderKeyVal(),val.toString()));
				}						
			}			
		} catch (Exception e) { 
			throw new EFException(e,instanceConfig.getInstanceID()+ " Kafka write data Exception!",ELEVEL.Dispose); 
		}
	}

	@Override
	public void flush() throws EFException {
		this.getconn().flush();
	}

	@Override
	public boolean create(String mainName, String storeId, InstanceConfig instanceConfig) {
		return true;
	}

	@Override
	public void delete(String instance, String storeId, String keyColumn, String keyVal) throws EFException {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeInstance(String instance, String storeId) { 
        DeleteTopicsOptions options = new DeleteTopicsOptions();
        options.timeoutMs(5000);  
        Properties conf = new Properties();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connectParams.getWhp().getHost());
		conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        AdminClient adminClient = AdminClient.create(conf);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(Resource.nodeConfig.getInstanceConfigs().get(instance).getWriteFields().get("topic").getDefaultvalue()), options); 
        Map<String, Boolean> map = new HashMap<>();
        try {
            for (Map.Entry<String, KafkaFuture<Void>> entry : deleteTopicsResult.topicNameValues().entrySet()) {
                String topic = entry.getKey();
                KafkaFuture<Void> future = entry.getValue();
                future.get(); 
                map.put(topic, !future.isCompletedExceptionally());
                log.info("remove kafka topic {} success!", topic);
            }
        } catch (Exception e) {
        	log.error("remove kafka topic exception!", e);
        }
	}

	@Override
	public void setAlias(String instance, String storeId, String aliasName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void optimize(String instance, String storeId) {
		// TODO Auto-generated method stub

	}

	@Override
	protected String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		String select = "";
		return select;
	}	

	@Override
	public boolean storePositionExists(String storeName) {
		return true;
	}

}
