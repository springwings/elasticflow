package org.elasticflow.writer.flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.TimeZone;

import org.elasticflow.config.GlobalParam.Mechanism;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.RiverField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.FNException;
import org.elasticflow.util.PipeNorms;
import org.elasticflow.writer.WriterFlowSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlFlow extends WriterFlowSocket {

	private final static Logger log = LoggerFactory.getLogger("MysqlFlow");

	public static MysqlFlow getInstance(ConnectParams connectParams) {
		MysqlFlow o = new MysqlFlow();
		o.INIT(connectParams);
		return o;
	}

	@Override
	public void write(WriterParam writerParam, PipeDataUnit unit, Map<String, RiverField> transParams, String instance,
			String storeId, boolean isUpdate) throws FNException {
		String table = Common.getStoreName(instance, storeId);
		boolean releaseConn = false;
		try { 
			PREPARE(false, false);
			if (!ISLINK())
				return;
			Connection conn = (Connection) GETSOCKET().getConnection(false);
			try (PreparedStatement statement = conn.prepareStatement(PipeNorms.getWriteSql(table, unit, transParams));) {
				statement.execute();
			} catch (Exception e) {
				log.error("PreparedStatement Exception", e);
			}
		} catch (Exception e) {
			log.error("write Exception", e);
		} finally {
			REALEASE(false, releaseConn);
		}
	}

	@Override
	public void flush() throws Exception {

	}

	@Override
	public String getNewStoreId(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		if(instanceConfig.getPipeParams().getWriteMechanism()==Mechanism.AB) {
			return abMechanism(mainName,isIncrement,instanceConfig);
		}else {
			return timeMechanism(mainName,isIncrement,instanceConfig);
		} 
	}

	@Override
	public boolean create(String instance, String storeId, Map<String, RiverField> transParams) {
		String name = Common.getStoreName(instance, storeId);
		String type = instance; 
		PREPARE(false, false);
		if (!ISLINK())
			return false;
		Connection conn = (Connection) GETSOCKET().getConnection(false);
		try (PreparedStatement statement = conn.prepareStatement(getTableSql(name, transParams));) {
			log.info("create Instance " + name + ":" + type);
			statement.execute();
			return true;
		} catch (Exception e) {
			log.error("create Instance " + name + ":" + type + " failed!", e);
			return false;
		} finally {
			REALEASE(false, false);
		}
	} 

	@Override
	public void delete(String instance, String storeId, String keyColumn, String keyVal) throws FNException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeInstance(String instance, String storeId) {
		String name = Common.getStoreName(instance, storeId); 
		PREPARE(false, false);
		if (!ISLINK())
			return;
		Connection conn = (Connection) GETSOCKET().getConnection(false);
		try (PreparedStatement statement = conn.prepareStatement("DROP table if exists "+name);) {
			log.info("Remove Instance " + name + " success!");
			statement.execute(); 
		} catch (Exception e) {
			log.error("Remove Instancee " + name + " failed!", e); 
		} finally {
			REALEASE(false, false);
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
	
	private String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		String select = "b";
		boolean releaseConn = false;
		PREPARE(false, false);
		if (!ISLINK())
			return select;
		Connection conn = (Connection) GETSOCKET().getConnection(false);
		String checkSql = " show tables like '"
				+ Common.getStoreName(mainName, "a") + "';";
		try (PreparedStatement statement = conn.prepareStatement(checkSql);) {
			try (ResultSet rs = statement.executeQuery();) {
				if (!rs.next())  
					select = "a";
			} catch (Exception e) {
				log.error("ResultSet Exception", e);
			}
		} catch (Exception e) {
			log.error("PreparedStatement Exception", e);
		} finally {
			REALEASE(false, releaseConn);
		}
		return select;
	}
	
	private String timeMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		long current=System.currentTimeMillis(); 
		return String.valueOf(current/(1000*3600*24)*(1000*3600*24)-TimeZone.getDefault().getRawOffset()); 
	}
	
	private String getTableSql(String instance, Map<String, RiverField> transParams) {
		StringBuilder sf = new StringBuilder();
		sf.append("create table " + instance + " (");
		for (Map.Entry<String, RiverField> e : transParams.entrySet()) {
			RiverField p = e.getValue();
			if (p.getName() == null)
				continue;
			sf.append(p.getAlias());
			sf.append(" " + p.getIndextype());
			sf.append(" ,");
			if (p.getIndexed() == "true") {
				sf.append("KEY `" + p.getName() + "` (`" + p.getName() + "`) USING BTREE ");
			}
		}
		String tmp = sf.substring(0, sf.length() - 1);
		return tmp + ");";
	}
}
