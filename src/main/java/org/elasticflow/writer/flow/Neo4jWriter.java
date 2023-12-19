package org.elasticflow.writer.flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.writer.WriterFlowSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

public class Neo4jWriter extends WriterFlowSocket {
	
	private final static Logger log = LoggerFactory.getLogger(Neo4jWriter.class);
	
	public static Neo4jWriter getInstance(ConnectParams connectParams) {
		Neo4jWriter o = new Neo4jWriter();
		o.initConn(connectParams);
		return o;
	}

	@Override
	public boolean create(String instance, String storeId, InstanceConfig instanceConfig) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public void write(InstanceConfig instanceConfig,PipeDataUnit unit, String instance,
			String storeId, boolean isUpdate) throws EFException {
		boolean releaseConn = false;
		try { 
			PREPARE(false, false, false);
			if (!ISLINK())
				return;
			Map<String, EFField> transParams = instanceConfig.getWriteFields();
			WriterParam writerParam = instanceConfig.getWriterParams();
			Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.writer);
			try (PreparedStatement statement = conn.prepareStatement(
					getWriteSQL(writerParam, unit, transParams));) {
				statement.execute();
			} catch (Exception e) { 
				throw new EFException(e,getWriteSQL(writerParam, unit, transParams),ELEVEL.Dispose);
			}
		} catch (Exception e) {  
			throw new EFException(e,instance+" neo4j error writing data",ELEVEL.Dispose);
		} finally {
			REALEASE(false, releaseConn);
		}
		
	}

	@Override
	public void delete(String instance, String storeId, String keyColumn, String keyVal) throws EFException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeInstance(String instance, String storeId) {
		log.info("no need to remove Instance."); 
	}

	@Override
	public void setAlias(String instance, String storeId, String aliasName) {
		log.info("no need to set Alias."); 
	} 

	@Override
	public void optimize(String instance, String storeId) {
		// TODO Auto-generated method stub
		
	}
	
	protected String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) throws EFException { 
		if (isIncrement)
			return "a";
		Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.writer);
		try (PreparedStatement statement = conn.prepareStatement("match(n) return n limit 1");) {
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				try (PreparedStatement statement2 = conn.prepareStatement("match (n) detach delete n");){
					statement2.execute();
					log.info("ab-Mechanism success remove neo4j instance {}.",mainName);
				} 
			} 
		} catch (Exception e) {
			log.error("ab-Mechanism try to remove neo4j instance {} exception!",mainName, e); 
		}  
		return "a";
	}
	
	private String getWriteSQL(WriterParam writerParam,PipeDataUnit unit,Map<String, EFField> transParams) { 
		JSONObject JO = writerParam.getCustomParams(); 
		String res="";
		for (Entry<String, Object> r : unit.getData().entrySet()) {
			String field = r.getKey();
			EFField transParam = transParams.get(field);
			if (transParam == null)
				transParam = transParams.get(field.toLowerCase());
			if (transParam == null)
				continue;
			if(writerParam.getDslParse().equals("condition") && transParam.getIndextype().equals("condition")) {
				res = JO.getString(String.valueOf(r.getValue()));
			}else {
				res = JO.toString().replace("#{"+field+"}", String.valueOf(r.getValue()));
			} 
			
		}
		return res;
	}

	@Override
	public boolean storePositionExists(String storeName) {
		return true;
	}

}
