package org.elasticflow.writer.flow;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.connect.VearchConnector;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.writer.WriterFlowSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.json.JSONObject;

/**
 * Vearch Flow Writer Manager
 * @author chengwen
 * @version 1.0 
 */

public class VearchFlow extends WriterFlowSocket {
	
	final static int flushSize = 100;

	final static int flushSecond = 3;
	
	protected long currentSec = Common.getNow(); 
	
	protected CopyOnWriteArrayList<Object> DATAS = new CopyOnWriteArrayList<Object>();
	 	
	private final static Logger log = LoggerFactory.getLogger("VearchFlow");
	
	private String curTable;
	
	public static VearchFlow getInstance(ConnectParams connectParams) {
		VearchFlow o = new VearchFlow();
		o.INIT(connectParams);
		return o;
	}	
	
	@Override
	public boolean create(String mainName, String storeId, InstanceConfig instanceConfig) {
		String name = Common.getStoreName(mainName, storeId);
		String type = mainName;
		PREPARE(false, false);
		if (!ISLINK())
			return false;
		VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(false);
		try {
			log.info("create Instance " + name + ":" + type);
			conn.createSpace(this.getTableMeta(name,instanceConfig));
			return true;
		} catch (Exception e) {
			log.error("create Instance " + name + ":" + type + " failed!", e);
			return false;
		} finally {
			REALEASE(false, false);
		}
	}


	@Override
	public boolean storePositionExists(String storeName) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void write(WriterParam writerParam, PipeDataUnit unit, Map<String, EFField> transParams, String instance,
			String storeId, boolean isUpdate) throws EFException {
		String table = Common.getStoreName(instance, storeId);
		try {
			if (!ISLINK())
				return;
			VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(false);	
			JSONObject row = new JSONObject();
			for (Entry<String, Object> r : unit.getData().entrySet()) {
				String field = r.getKey();
				if (r.getValue() == null)
					continue;
				Object value = r.getValue();
				EFField transParam = transParams.get(field);
				if(transParam.getIndextype().equals("vector")) {
					JSONObject _feature = new JSONObject();
					_feature.put("feature", Common.parseFieldValue(value,transParam));
					row.put(transParam.getAlias(),_feature);
				}else {
					row.put(transParam.getAlias(), Common.parseFieldValue(value,transParam));
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
			log.error("write Exception", e);
		} 
	}

	@Override
	public void delete(String instance, String storeId, String keyColumn, String keyVal) throws EFException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeInstance(String instance, String storeId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setAlias(String instance, String storeId, String aliasName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void flush() throws Exception {
		if (this.isBatch) {
			synchronized (this.DATAS) {
				if(this.DATAS.size()>0) {
					VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(false);	
					conn.writeBatch(this.curTable, this.DATAS);
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
		JSONObject properties = new JSONObject();
		Map<String, EFField> writefields = instanceConfig.getWriteFields();
		for (Map.Entry<String, EFField> entry : writefields.entrySet()) {
			if(entry.getValue().getDsl()!=null) {
				properties.put(entry.getKey(),entry.getValue().getDsl());
			}else {
				properties.put(entry.getKey(),"{\"type\":\""+entry.getValue().getIndextype()+"\"}");
			}			
		} 
		tableMeta.put("properties", properties);
		return tableMeta;
		
	}

}
