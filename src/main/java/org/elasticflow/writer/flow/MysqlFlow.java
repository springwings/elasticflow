package org.elasticflow.writer.flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

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

/**
 * Mysql flow Writer Manager
 * @author chengwen
 * @version 1.0 
 */

public class MysqlFlow extends WriterFlowSocket {

	private final static Logger log = LoggerFactory.getLogger("MysqlFlow");

	final static int flushSize = 100;

	final static int flushSecond = 3;

	private CopyOnWriteArrayList<String> sqlData = new CopyOnWriteArrayList<>();

	private long currentSec = Common.getNow();

	public static MysqlFlow getInstance(ConnectParams connectParams) {
		MysqlFlow o = new MysqlFlow();
		o.INIT(connectParams);
		return o;
	}

	@Override
	public void write(WriterParam writerParam, PipeDataUnit unit, Map<String, EFField> transParams, String instance,
			String storeId, boolean isUpdate) throws EFException {
		String table = Common.getStoreName(instance, storeId);
		try {
			if (!ISLINK())
				return;
			if (this.isBatch) {
				sqlData.add(PipeNormsUtil.getWriteSqlTailData(table, unit, transParams));
				if (currentSec - Common.getNow() > flushSecond || sqlData.size() > flushSize) {
					StringBuffer sb = new StringBuffer();
					for (String s : sqlData) {
						sb.append(s + ",");
					}
					sqlData.clear();
					currentSec = Common.getNow();
					this.insertDb(
							PipeNormsUtil.getWriteSqlHead(table, unit, transParams) + sb.substring(0, sb.length() - 1));
				}
			} else {
				this.insertDb(PipeNormsUtil.getWriteSql(table, unit, transParams));
			}
		} catch (Exception e) {
			log.error("write Exception", e);
		}
	}

	private void insertDb(String sql) {
		Connection conn = (Connection) GETSOCKET().getConnection(false);
		try (PreparedStatement statement = conn.prepareStatement(sql);) {
			statement.execute();
		} catch (Exception e) {
			log.error("PreparedStatement Exception", e);
		}
	}

	@Override
	public void flush() throws Exception {

	}

	@Override
	public boolean create(String mainName, String storeId, InstanceConfig instanceConfig) {
		String name = Common.getStoreName(mainName, storeId);
		String type = mainName;
		PREPARE(false, false);
		if (!ISLINK())
			return false;
		if(!this.storePositionExists(name)) {
			Connection conn = (Connection) GETSOCKET().getConnection(false);
			try (PreparedStatement statement = conn.prepareStatement(this.getTableSql(name, instanceConfig));) {
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
		return true;
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
		try (PreparedStatement statement = conn.prepareStatement("DROP table if exists " + name);) {
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

	@Override
	protected String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		String select = "b";
		boolean releaseConn = false;
		PREPARE(false, false);
		if (!ISLINK())
			return select;
		Connection conn = (Connection) GETSOCKET().getConnection(false);
		String checkSql = " show tables like '" + Common.getStoreName(mainName, "a") + "';";
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
	 * create table sql
	 * 
	 * @param instance
	 * @param instanceConfig
	 * @return
	 */
	private String getTableSql(String instance, InstanceConfig instanceConfig) {
		StringBuilder sf = new StringBuilder();
		sf.append("create table " + instance + " (");
		Map<String, EFField> transParams = instanceConfig.getWriteFields();
		for (Map.Entry<String, EFField> e : transParams.entrySet()) {
			EFField p = e.getValue();
			if (p.getAlias() == null)
				continue;
			sf.append(p.getAlias());
			sf.append(" " + p.getIndextype());
			if (p.getIndextype().toLowerCase().equals("timestamp"))
				sf.append(" DEFAULT CURRENT_TIMESTAMP");
			sf.append(" ,");
			if (p.getIndexed().equals("true") && !p.getAlias().equals(instanceConfig.getWriterParams().getWriteKey())) {
				sf.append("KEY `" + p.getAlias() + "` (`" + p.getAlias() + "`) ,");
			}
		}
		sf.append("PRIMARY KEY `" + instanceConfig.getWriterParams().getWriteKey() + "` (`"
				+ instanceConfig.getWriterParams().getWriteKey() + "`) USING BTREE ");
		String tmp = sf.substring(0, sf.length() - 1);
		return tmp + ");";
	}

	@Override
	public boolean storePositionExists(String storeName) {
		String checkdatabase = "show databases like \"" + storeName + "\"";
		Connection conn = (Connection) GETSOCKET().getConnection(false);
		try (PreparedStatement stat = conn.prepareStatement(checkdatabase);) {
			ResultSet resultSet = stat.executeQuery();
			if (resultSet.next()) {
				return true;
			}
		} catch (Exception e) {
			log.error("store Position check Exception", e);
		}
		return false;
	}
}
