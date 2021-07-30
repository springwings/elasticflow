package org.elasticflow.writer.flow;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.field.FieldHandler;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
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

	@Override
	public void write(WriterParam writerParam, PipeDataUnit unit, Map<String, EFField> transParams, String instance,
			String storeId, boolean isUpdate) throws EFException {
		try {
			if (!ISLINK())
				return;
			@SuppressWarnings("unchecked")
			KafkaProducer<String, String> conn = (KafkaProducer<String, String>) GETSOCKET().getConnection(END_TYPE.writer);
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
					if (transParam.getParamtype().contains("org.elasticflow.field.handler")) {
						FieldHandler<?> _v = (FieldHandler<?>) Common.parseFieldValue(String.valueOf(val),
								transParam);	
						conn.send(new ProducerRecord<String, String>(transParams.get("topic").getDefaultvalue(), unit.getReaderKeyVal(),_v.toString()));
					}else {
						conn.send(new ProducerRecord<String, String>(transParams.get("topic").getDefaultvalue(), unit.getReaderKeyVal(),val.toString()));
					}
				}						
			}
			
		} catch (Exception e) {
			log.error("write Exception", e);
		}
	}

	@Override
	public void flush() throws Exception {
		@SuppressWarnings("unchecked")
		KafkaProducer<String, String> conn = (KafkaProducer<String, String>) GETSOCKET().getConnection(END_TYPE.writer);
		conn.flush();
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