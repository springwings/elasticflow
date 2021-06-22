package org.elasticflow.writer.flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.TimeZone;

import org.elasticflow.config.GlobalParam.MECHANISM;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.PipeNormsUtil;
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
	public void write(WriterParam writerParam, PipeDataUnit unit, Map<String, EFField> transParams, String instance,
			String storeId, boolean isUpdate) throws EFException {
		String table = Common.getStoreName(instance, storeId);
		boolean releaseConn = false;
		try { 
			PREPARE(false, false);
			if (!ISLINK())
				return;
			Connection conn = (Connection) GETSOCKET().getConnection(false);
			try (PreparedStatement statement = conn.prepareStatement(PipeNormsUtil.getWriteSql(table, unit, transParams));) {
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
		if(instanceConfig.getPipeParams().getWriteMechanism()==MECHANISM.AB) {
			return abMechanism(mainName,isIncrement,instanceConfig);
		}else {
			return timeMechanism(mainName,isIncrement,instanceConfig);
		} 
	}

	@Override
	public boolean create(String instance, String storeId, InstanceConfig instanceConfig) {
		String name = Common.getStoreName(instance, storeId);
		String type = instance; 
		PREPARE(false, false);
		if (!ISLINK())
			return false;
		Connection conn = (Connection) GETSOCKET().getConnection(false);
		try (PreparedStatement statement = conn.prepareStatement(getTableSql(name, instanceConfig.getWriteFields()));) {
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
	public void delete(String instance, String storeId, String keyColumn, String keyVal) throws EFException {
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
	
	/**
	 * Get the time-stamp of 0 o'clock every day/month
	 * Maintain expired instances
	 * @param mainName
	 * @param isIncrement
	 * @param instanceConfig
	 * @return time-stamp of second
	 */
	private String timeMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		int[] timeSpan=instanceConfig.getPipeParams().getKeepNums();
		String storeId;
		long foward;
		long current = System.currentTimeMillis();
		LocalDate lnow = LocalDate.now();
		if(timeSpan[0]==0) {
			foward = lnow.minusDays(timeSpan[1]).atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli();
			storeId = String.valueOf((current / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset())/1000);
		}else {
			Long startDay = Common.getMonthStartTime(current, "GMT+8:00");
			storeId = String.valueOf(startDay/1000);			
			LocalDate ldate = LocalDate.of(lnow.getYear(), lnow.getMonth(), 1);
			foward = ldate.minusMonths(timeSpan[1]).atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli();			
		}
		//remove out of range data		
		String keepLastTime = String.valueOf(foward/1000);
		String iName = Common.getStoreName(mainName, keepLastTime);
		try {
			this.removeInstance(mainName, keepLastTime);
		} catch (Exception e) {
			log.error("remove instance　"+iName+" Exception!", e);
		}		
		return storeId;
	}
	
	private String getTableSql(String instance, Map<String, EFField> transParams) {
		StringBuilder sf = new StringBuilder();
		sf.append("create table " + instance + " (");
		for (Map.Entry<String, EFField> e : transParams.entrySet()) {
			EFField p = e.getValue();
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
