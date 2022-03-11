package org.elasticflow.writer.flow;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.writer.WriterFlowSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase flow Writer Manager
 * @author chengwen
 * @version 1.0 
 */
@ThreadSafe
public class HbaseWriter extends WriterFlowSocket { 
	
	final String DEFAULT_KEY = "tableColumnFamily";
	private List<Put> data = new CopyOnWriteArrayList<Put>();   
	private String columnFamily;
	private final static Logger log = LoggerFactory.getLogger("HBaseFlow"); 
	
	public static HbaseWriter getInstance(ConnectParams connectParams) {
		HbaseWriter o = new HbaseWriter();
		o.initConn(connectParams);
		return o;
	}
	
	@Override
	public void initConn(ConnectParams connectParams) {
		this.connectParams = connectParams;  
		String tableColumnFamily = connectParams.getWhp().getDefaultValue().getString(DEFAULT_KEY);
		if (tableColumnFamily != null && tableColumnFamily.length() > 0) {
			String[] strs = tableColumnFamily.split(":"); 
			if (strs != null && strs.length > 1)
				this.columnFamily = strs[1];
		}
		this.poolName = connectParams.getWhp().getPoolName(connectParams.getL1Seq());
	} 
	 
	@Override
	public void write(InstanceConfig instanceConfig,PipeDataUnit unit,String instantcName, String storeId,boolean isUpdate) throws EFException { 
		if (unit.getData().size() == 0){
			log.info("Empty IndexUnit for " + instantcName + " " + storeId);
			return;
		}  
		Map<String, EFField> transParams = instanceConfig.getWriteFields();
		String id = unit.getReaderKeyVal(); 
		Put put = new Put(Bytes.toBytes(id));
		
		for(Entry<String, Object> r:unit.getData().entrySet()){
			String field = r.getKey(); 
			if (r.getValue() == null)
				continue;
			String value = String.valueOf(r.getValue());
			if (field.equalsIgnoreCase("update_time") && value!=null)
				value = String.valueOf(System.currentTimeMillis());
			
			if (value == null)
				continue;
			
			EFField transParam = transParams.get(field);
			if (transParam == null)
				transParam = transParams.get(field.toLowerCase());
			if (transParam == null)
				transParam = transParams.get(field.toUpperCase());
			if (transParam == null)
				continue; 
			put.addColumn(Bytes.toBytes(this.columnFamily), Bytes.toBytes(transParam.getAlias()),
					Bytes.toBytes(value));  
		} 
		synchronized (data) {
			data.add(put); 
		}
	} 

	@Override
	public void delete(String instance, String storeId,String keyColumn, String keyVal) throws EFException {
		
	}

	@Override
	public void removeInstance(String instanceName, String batchId) {
		
	}

	@Override
	public void setAlias(String instanceName, String batchId, String aliasName) {

	}

	@Override
	public synchronized void flush() throws EFException { 
		try {
			getTable().put(data);
		} catch (IOException e) {
			throw new EFException(e, ELEVEL.Termination);
		}
		data.clear();
	}

	@Override
	public void optimize(String instantcName, String storeId) {
		
	}
 
	@Override
	public boolean create(String instantcName, String batchId, InstanceConfig instanceConfig) {
		return true;
	}

	@Override
	public String getNewStoreId(String mainName,boolean isIncrement,InstanceConfig instanceConfig) {
		// TODO Auto-generated method stub
		return "a";
	}

	@Override
	protected String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean storePositionExists(String storeName) {
		return true;
	}
	
	private Table getTable() throws EFException { 
		return (Table) GETSOCKET().getConnection(END_TYPE.writer);
	}
 
}
