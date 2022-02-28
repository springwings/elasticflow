package org.elasticflow.writer.flow;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.connection.VearchConnector;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.EFException.ETYPE;
import org.elasticflow.writer.WriterFlowSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * Vearch Flow Writer Manager
 * @author chengwen
 * @version 1.0 
 */

public class VearchWriter extends WriterFlowSocket {
	
	final static int flushSize = 100;

	final static int flushSecond = 3;
	
	protected long currentSec = Common.getNow(); 
	
	protected CopyOnWriteArrayList<Object> DATAS = new CopyOnWriteArrayList<Object>();
	 	
	private final static Logger log = LoggerFactory.getLogger("VearchFlow");
	
	private String curTable;
	
	public static VearchWriter getInstance(ConnectParams connectParams) {
		VearchWriter o = new VearchWriter();
		o.initConn(connectParams);
		return o;
	}	
	
	@Override
	public boolean create(String mainName, String storeId, InstanceConfig instanceConfig) throws EFException{
		String name = Common.getStoreName(mainName, storeId);
		String type = mainName;
		PREPARE(false, false);
		if (!ISLINK())
			return false;
		if(!this.storePositionExists(name)) {
			VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);
			try {
				log.info("create Instance " + name + ":" + type);
				conn.createSpace(this.getTableMeta(name,instanceConfig));
				return true;
			} catch (Exception e) {
				throw new EFException(e,ELEVEL.Termination,ETYPE.RESOURCE_ERROR);	
			} finally {
				REALEASE(false, false);
			}
		}
		return true;
	}


	@Override
	public boolean storePositionExists(String storeName) {		
		VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);
		return conn.checkSpaceExists(storeName);
	}

	@Override
	public void write(InstanceConfig instanceConfig,PipeDataUnit unit, String instance,
			String storeId, boolean isUpdate) throws EFException {
		String table = Common.getStoreName(instance, storeId);
		if (!ISLINK())
			return;
		Map<String, EFField> transParams = instanceConfig.getWriteFields();
		VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);
		try {				
			JSONObject row = new JSONObject();
			for (Entry<String, Object> r : unit.getData().entrySet()) {
				String field = r.getKey();
				if (r.getValue() == null)
					continue;				
				Object value = r.getValue();
				EFField transParam = transParams.get(field);
				if(transParam==null)
					continue;
				if(transParam.getIndextype().equals("vector")) {
					JSONObject _feature = new JSONObject();
					_feature.put("feature", value);
					row.put(transParam.getAlias(),_feature);
				}else {
					row.put(transParam.getAlias(), value);
				}		
			} 
			if (this.isBatch) {
				this.curTable = table;
				this.DATAS.add("{\"index\": {\"_id\": \""+unit.getReaderKeyVal()+"\"}}");				
				this.DATAS.add(row);
				if (this.DATAS.size()>flushSize || Common.getNow()-currentSec > flushSecond) {
					synchronized (this.DATAS) {
						conn.writeBatch(table, this.DATAS);
						currentSec = Common.getNow();
						this.DATAS.clear();
					}
				}				
			} else {	
				conn.writeSingle(table, row);
			}
		} catch (Exception e) {
			if(e.getMessage().contains("spaceName param not build")) {
				throw new EFException(e,ELEVEL.Dispose,ETYPE.WRITE_POS_NOT_FOUND);
			}else {
				throw new EFException(e,ELEVEL.Dispose);
			} 
		} 
	}
	
	public void deleteAndInsert() throws Exception {
		VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);
		synchronized (this.DATAS) {			
			conn.deleteBatch(this.curTable, this.DATAS);
			conn.writeBatch(this.curTable, this.DATAS);
			currentSec = Common.getNow();
			this.DATAS.clear();
		}		
	}

	@Override
	public void delete(String instance, String storeId, String keyColumn, String keyVal) throws EFException {
		String name = Common.getStoreName(instance, storeId);
		try {
			VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);	
			conn.deleteSpace(name);
		} catch (Exception e) {
			throw new EFException(e,ELEVEL.Termination);
		} 
	}

	@Override
	public void removeInstance(String instance, String storeId) {
		String name = Common.getStoreName(instance, storeId);
		PREPARE(false, false);
		if (!ISLINK())
			return;
		VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);	
		try {
			conn.deleteSpace(name);
			log.info("Remove Instance " + name + " success!");
		} catch (Exception e) {
			log.error("Remove Instancee " + name + " failed!", e);
		} finally {
			REALEASE(false, false);
		}
	}

	@Override
	protected String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) throws EFException{
		String select = "a";
		boolean a = this.storePositionExists(Common.getStoreName(mainName, "a"));
		boolean b = this.storePositionExists(Common.getStoreName(mainName, "b"));
		
		if (isIncrement) {
			if(a && b) {
				select = "a";
			}else {
				select = a ? "a" : (b ? "b" : "a");
			}
			if ((select.equals("a") && !a) || (select.equals("b") && !b)) {
				this.create(mainName, select, instanceConfig);
			}			
		}else {
			this.create(mainName, select, instanceConfig);
		} 		
		return select;
	}

	@Override
	public void setAlias(String instance, String storeId, String aliasName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void flush() throws EFException {
		if (this.isBatch) {
			synchronized (this.DATAS) {
				if(this.DATAS.size()>0) {
					VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);	
					try {
						conn.writeBatch(this.curTable, this.DATAS);
					} catch (Exception e) {
						if(e.getMessage().contains("spaceName param not build")) {
							throw new EFException(e,ELEVEL.Dispose,ETYPE.WRITE_POS_NOT_FOUND);
						}else {
							throw new EFException(e,ELEVEL.Termination);
						}
					} 
					currentSec = Common.getNow();
					this.DATAS.clear();
				}				
			}
		}		
	}

	@Override
	public void optimize(String instance, String storeId) {
		// TODO Auto-generated method stub
		
	}
	
	
	private JSONObject getTableMeta(String tableName,InstanceConfig instanceConfig) {		
		JSONObject tableMeta = new JSONObject();
		if(instanceConfig.getWriterParams().getStorageStructure() != null && 
				instanceConfig.getWriterParams().getStorageStructure().size()>0) {
			tableMeta = instanceConfig.getWriterParams().getStorageStructure();
			tableMeta.put("name", tableName);
		}else {			
			tableMeta.put("name", tableName);
			tableMeta.put("partition_num", 1);
			tableMeta.put("replica_num", 1);
			
			JSONObject engine = new JSONObject();
			engine.put("name","gamma");
			engine.put("index_size",200000);
			engine.put("id_type","String");
			engine.put("retrieval_type","IVFPQ");
			
			JSONObject retrieval_param = new JSONObject();
			retrieval_param.put("metric_type","L2");
			retrieval_param.put("ncentroids",2048);
			retrieval_param.put("nsubvector",32);
			engine.put("retrieval_param",retrieval_param);
			
			tableMeta.put("engine", engine);			
		}
		JSONObject properties = new JSONObject();
		Map<String, EFField> writefields = instanceConfig.getWriteFields();
		for (Map.Entry<String, EFField> entry : writefields.entrySet()) {
			if(entry.getValue().getDsl()!=null) {
				properties.put(entry.getKey(),JSONObject.parse(entry.getValue().getDsl()));
			}else { 
				JSONObject fields = new JSONObject();
				fields.put("type", entry.getValue().getIndextype());
				if(entry.getValue().getIndexed().equals("true")) {
					fields.put("index", true);
				}
				properties.put(entry.getKey(),fields);
			}			
		} 
		tableMeta.put("properties", properties);
		return tableMeta;
		
	}

}
