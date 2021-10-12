package org.elasticflow.writer.flow;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.writer.WriterFlowSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka flow Writer Manager
 * @author chengwen
 * @version 1.0 
 */

public class KafkaFlow extends WriterFlowSocket {

	private final static Logger log = LoggerFactory.getLogger("KafkaFlow");

	public static KafkaFlow getInstance(ConnectParams connectParams) {
		KafkaFlow o = new KafkaFlow();
		o.INIT(connectParams);
		return o;
	}
	
	@SuppressWarnings("unchecked")
	private KafkaProducer<String, String> getconn() {
		return (KafkaProducer<String, String>) GETSOCKET().getConnection(END_TYPE.writer);
	}
	
	@Override
	public void write(WriterParam writerParam, PipeDataUnit unit, Map<String, EFField> transParams, String instance,
			String storeId, boolean isUpdate) throws EFException {
		if (!ISLINK())
			return; 
		KafkaProducer<String, String> conn = this.getconn();
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
					conn.send(new ProducerRecord<String, String>(transParams.get("topic").getDefaultvalue(), unit.getReaderKeyVal(),val.toString()));
				}						
			}			
		} catch (Exception e) {
			log.error("write Exception", e);
			throw new EFException(e,ELEVEL.Dispose);
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
