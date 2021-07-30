package org.elasticflow.writer.flow;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseNosqlParam;
import org.elasticflow.util.EFException;
import org.elasticflow.writer.WriterFlowSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase flow Writer Manager
 * @author chengwen
 * @version 1.0 
 */
@ThreadSafe
public class HbaseFlow extends WriterFlowSocket { 
	 
	private List<Put> data = new CopyOnWriteArrayList<Put>();   
	private String columnFamily;
	private final static Logger log = LoggerFactory.getLogger("HBaseFlow"); 
	
	public static HbaseFlow getInstance(ConnectParams connectParams) {
		HbaseFlow o = new HbaseFlow();
		o.INIT(connectParams);
		return o;
	}
	
	@Override
	public void INIT(ConnectParams connectParams) {
		this.connectParams = connectParams;  
		String tableColumnFamily = ((WarehouseNosqlParam) connectParams.getWhp()).getDefaultValue();
		if (tableColumnFamily != null && tableColumnFamily.length() > 0) {
			String[] strs = tableColumnFamily.split(":"); 
			if (strs != null && strs.length > 1)
				this.columnFamily = strs[1];
		}
		this.poolName = connectParams.getWhp().getPoolName(connectParams.getL1Seq());
	} 
	
	
	private Table getTable() { 
		return (Table) GETSOCKET().getConnection(END_TYPE.writer);
	}

	 
	@Override
	public void write(WriterParam writerParam,PipeDataUnit unit,Map<String, EFField> transParams, String instantcName, String storeId,boolean isUpdate) throws EFException { 
		if (unit.getData().size() == 0){
			log.info("Empty IndexUnit for " + instantcName + " " + storeId);
			return;
		}  
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
	public void flush() throws Exception { 
		synchronized (data) {
			getTable().put(data);
			data.clear();
		} 
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
 
}
