package org.elasticflow.writer.flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.config.GlobalParam.ETYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.instance.PipeUtil;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.writer.WriterFlowSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mysql flow Writer Manager
 * 
 * @author chengwen
 * @version 1.0
 */

public class MysqlWriter extends WriterFlowSocket {

	private final static Logger log = LoggerFactory.getLogger(MysqlWriter.class);

	final static int flushSize = 100;

	final static int flushSecond = 3;

	private CopyOnWriteArrayList<String> sqlData = new CopyOnWriteArrayList<>();

	private long currentSec = Common.getNow();

	public static MysqlWriter getInstance(ConnectParams connectParams) {
		MysqlWriter o = new MysqlWriter();
		o.initConn(connectParams);
		return o;
	}

	@Override
	public void write(InstanceConfig instanceConfig, PipeDataUnit unit, String instance, String storeId,
			boolean isUpdate) throws EFException {
		String table = TaskUtil.getStoreName(instance, storeId);
		Map<String, EFField> transParams = instanceConfig.getWriteFields();
		try {
			if (!connStatus())
				return;
			if (this.isBatch) {
				sqlData.add(PipeUtil.getWriteSqlTailData(table, unit, transParams));
				if (currentSec - Common.getNow() > flushSecond || sqlData.size() > flushSize) {
					StringBuffer sb = new StringBuffer();
					for (String s : sqlData) {
						sb.append(s + ",");
					}
					sqlData.clear();
					currentSec = Common.getNow();
					this.insertDb(
							PipeUtil.getWriteSqlHead(table, unit, transParams) + sb.substring(0, sb.length() - 1));
				}
			} else {
				this.insertDb(PipeUtil.getWriteSql(table, unit, transParams));
			}
		} catch (Exception e) {
			throw new EFException(e, "mysql write data exception", ELEVEL.Dispose);
		}
	}

	@Override
	public boolean create(String mainName, String storeId, InstanceConfig instanceConfig) throws EFException {
		String name = TaskUtil.getStoreName(mainName, storeId);
		String type = mainName;
		PREPARE(false, false, false);
		if (!connStatus())
			return false;
		if (!this.storePositionExists(name)) {
			Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.writer);
			try (PreparedStatement statement = conn.prepareStatement(this.getTableSql(name, instanceConfig));) {
				log.info("create mysql instance {}:{}", name, type);
				statement.execute();
				return true;
			} catch (Exception e) {
				throw new EFException(e, "mysql create instance " + name + ":" + type + " exception",
						ELEVEL.Termination, ETYPE.RESOURCE_ERROR);
			} finally {
				releaseConn(false, false);
			}
		}
		return true;
	}

	@Override
	public void delete(String instance, String storeId, String keyColumn, String keyVal) throws EFException {

	}

	@Override
	public void removeShard(String instance, String storeId) throws EFException {
		String name = TaskUtil.getStoreName(instance, storeId);
		PREPARE(false, false, false);
		if (!connStatus())
			return;
		Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.writer);
		try (PreparedStatement statement = conn.prepareStatement("DROP table if exists " + name);) {
			log.info("remove mysql instance {}:{} success!", name, instance);
			statement.execute();
		} catch (Exception e) {
			log.error("remove mysql instance {}:{} exception!", name, instance, e);
		} finally {
			releaseConn(false, false);
		}
	}

	@Override
	public void setAlias(String instance, String storeId, String aliasName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void optimize(String instance, String storeId) {
		String name = TaskUtil.getStoreName(instance, storeId);
		PREPARE(false, false, false);
		try {
			if (connStatus() && this.storePositionExists(name)) {
				Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.writer);
				try (PreparedStatement statement = conn.prepareStatement(this.getTableSql(name, instanceConfig));) {
					log.info("create mysql instance {}:{}", name, instance);
					statement.execute();
				} catch (Exception e) {
					throw new EFException(e, "mysql optimze table exception", ELEVEL.Termination, ETYPE.RESOURCE_ERROR);
				} finally {
					releaseConn(false, false);
				}
			}
		} catch (Exception e) {
			log.warn("optimize mysql instance {}:{} exception!", name, instance, e);
		}
	}

	@Override
	public boolean storePositionExists(String storeName) throws EFException {
		String checkdatabase = "show tables like \"" + storeName + "\"";
		Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.writer);
		try (PreparedStatement stat = conn.prepareStatement(checkdatabase);) {
			ResultSet rs = stat.executeQuery();
			if (rs.next()) {
				return true;
			}
		} catch (Exception e) {
			log.error("mysql store position {} check exists exception", storeName, e);
		}
		return false;
	}

	@Override
	protected String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig)
			throws EFException {
		String select = "a";
		boolean clearConn = false;
		PREPARE(false, false, false);
		if (!connStatus())
			return select;
		Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.writer);
		String checkSql = "show tables like \"" + TaskUtil.getStoreName(mainName, select) + "\";";
		try (PreparedStatement statement = conn.prepareStatement(checkSql);) {
			try (ResultSet rs = statement.executeQuery();) {
				if (!rs.next()) {
					checkSql = "show tables like \"" + TaskUtil.getStoreName(mainName, "b") + "\";";
					try (PreparedStatement stat2 = conn.prepareStatement(checkSql);) {
						ResultSet rs2 = stat2.executeQuery();
						if (rs2.next())
							select = "b";
					}
				}
			} catch (Exception e) {
				log.error("instance {}, mysql ab Mechanism exception", mainName, e);
			}
		} catch (Exception e) {
			log.error("instance {}, PreparedStatement exception", mainName, e);
		} finally {
			releaseConn(false, clearConn);
		}
		if (!isIncrement) {
			if (select == "a")
				return "b";
			return "a";
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
		if (instanceConfig.getWriterParams().getStorageStructure() != null
				&& instanceConfig.getWriterParams().getStorageStructure().size() > 0) {
			String tablesql = "create table " + instance;
			tablesql += instanceConfig.getWriterParams().getStorageStructure().getString("tablemeta");
			return tablesql;
		} else {
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
				if (p.getIndexed().equals("true")
						&& !p.getAlias().equals(instanceConfig.getWriterParams().getWriteKey())) {
					sf.append("KEY `" + p.getAlias() + "` (`" + p.getAlias() + "`) ,");
				}
			}
			sf.append("PRIMARY KEY `" + instanceConfig.getWriterParams().getWriteKey() + "` (`"
					+ instanceConfig.getWriterParams().getWriteKey() + "`) USING BTREE ");
			String tmp = sf.substring(0, sf.length() - 1);
			return tmp + ");";
		}
	}

	private void insertDb(String sql) throws EFException {
		Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.writer);
		try (PreparedStatement statement = conn.prepareStatement(sql);) {
			statement.execute();
		} catch (Exception e) {
			log.error("insert data Exception", e);
		}
	}

}
