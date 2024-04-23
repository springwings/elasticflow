package org.elasticflow.writer.flow;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.config.GlobalParam.ETYPE;
import org.elasticflow.config.GlobalParam.RESOURCE_STATUS;
import org.elasticflow.connection.sockets.VearchConnector;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.EFException;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.writer.WriterFlowSocket;
import org.elasticflow.yarn.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * Vearch Flow Writer Manager
 * 
 * @author chengwen
 * @version 1.0
 */

public class VearchWriter extends WriterFlowSocket {

	protected CopyOnWriteArrayList<Object> DATAS = new CopyOnWriteArrayList<Object>();

	private final static Logger log = LoggerFactory.getLogger("VearchFlow");

	private String curTable;

	public static VearchWriter getInstance(ConnectParams connectParams) {
		VearchWriter o = new VearchWriter();
		o.initConn(connectParams);
		return o;
	}

	@Override
	public boolean create(String mainName, String storeId, InstanceConfig instanceConfig) throws EFException {
		String name = TaskUtil.getStoreName(mainName, storeId);
		PREPARE(false, false, false);
		if (!connStatus())
			return false;
		if (!this.storePositionExists(name)) {
			VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);
			try {
				log.info("create vearch instance store position {}:{}", name, mainName);
				conn.createSpace(this.getTableMeta(name, instanceConfig));
				return true;
			} catch (Exception e) {
				throw new EFException(e,
						"create vearch instance store position " + name + ":" + mainName + " exception",
						ELEVEL.Termination, ETYPE.RESOURCE_ERROR);
			} finally {
				releaseConn(false, false);
			}
		}
		return true;
	}

	@Override
	public boolean storePositionExists(String storeName) throws EFException {
		VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);
		return conn.checkSpaceExists(storeName);
	}

	@Override
	public void write(InstanceConfig instanceConfig, PipeDataUnit unit, String instance, String storeId,
			boolean isUpdate) throws EFException {
		String table = TaskUtil.getStoreName(instance, storeId);
		if (!connStatus())
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
				if (transParam == null)
					continue;
				if (transParam.getIndextype().equals("vector")) {
					JSONObject _feature = new JSONObject();
					_feature.put("feature", value);
					row.put(transParam.getAlias(), _feature);
				} else {
					row.put(transParam.getAlias(), value);
				}
			}
			if (this.isBatch) {
				this.curTable = table;
				this.DATAS.add("{\"index\": {\"_id\": \"" + unit.getReaderKeyVal() + "\"}}");
				this.DATAS.add(row);
			} else {
				conn.writeSingle(table, row);
			}
		} catch (Exception e) {
			if (e.getMessage().contains("spaceName param not build")) {
				throw new EFException(e, "vearch write data exception", ELEVEL.Termination, ETYPE.RESOURCE_ERROR);
			} else {
				throw new EFException(e, ELEVEL.Dispose);
			}
		}
	}

	public void deleteAndInsert() throws Exception {
		VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);
		conn.deleteBatch(this.curTable, this.DATAS);
		conn.writeBatch(this.curTable, this.DATAS);
		this.DATAS.clear();
	}

	@Override
	public void delete(String instance, String storeId, String keyColumn, String keyVal) throws EFException {
		String name = TaskUtil.getStoreName(instance, storeId);
		try {
			VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);
			conn.deleteSpace(name);
		} catch (Exception e) {
			throw new EFException(e, ELEVEL.Termination);
		}
	}

	@Override
	public void removeShard(String instance, String storeId) throws EFException {
		String name = TaskUtil.getStoreName(instance, storeId);
		PREPARE(false, false, false);
		if (!connStatus())
			return;
		VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);
		try {
			conn.deleteSpace(name);
			log.info("remove vearch instance {} success!", name);
		} catch (Exception e) {
			log.error("remove vearch instance {} exception!", name, e);
		} finally {
			releaseConn(false, false);
		}
	}

	@Override
	protected String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig)
			throws EFException {
		String select = "a";
		boolean a = this.storePositionExists(TaskUtil.getStoreName(mainName, "a"));
		boolean b = this.storePositionExists(TaskUtil.getStoreName(mainName, "b"));

		if (isIncrement) {
			if (a && b) {
				select = "a";
			} else {
				select = a ? "a" : (b ? "b" : "a");
			}
			if ((select.equals("a") && !a) || (select.equals("b") && !b)) {
				this.create(mainName, select, instanceConfig);
			}
		} else {
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
			synchronized (this) {
				if (this.DATAS.size() > 0) {
					VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.writer);
					try {
						conn.writeBatch(this.curTable, this.DATAS);
						Resource.resourceStates.get(connectParams.getWhp().getAlias()).put("status",RESOURCE_STATUS.Normal.name());
					} catch (Exception e) {
						if (e.getMessage().contains("spaceName param not build")) {
							throw new EFException(e, conn.getHosts(), ELEVEL.Termination,
									ETYPE.RESOURCE_ERROR);
						} else {
							Resource.resourceStates.get(connectParams.getWhp().getAlias()).put("status",RESOURCE_STATUS.Warning.name());
							throw new EFException(e,Resource.nodeConfig.getWarehouse().get(connectParams.getWhp().getAlias()).getHost(), ELEVEL.Termination);
						}
					}
					this.DATAS.clear();
				}
			}
		}
	}

	@Override
	public void optimize(String instance, String storeId) {
		// TODO Auto-generated method stub

	}

	private JSONObject getTableMeta(String tableName, InstanceConfig instanceConfig) {
		JSONObject tableMeta = new JSONObject();
		if (instanceConfig.getWriterParams().getStorageStructure() != null
				&& instanceConfig.getWriterParams().getStorageStructure().size() > 0) {
			tableMeta = instanceConfig.getWriterParams().getStorageStructure();
			tableMeta.put("name", tableName);
		} else {
			tableMeta.put("name", tableName);
			tableMeta.put("partition_num", 1);
			tableMeta.put("replica_num", 1);

			JSONObject engine = new JSONObject();
			engine.put("name", "gamma");
			engine.put("index_size", 200000);
			engine.put("id_type", "String");
			engine.put("retrieval_type", "IVFPQ");

			JSONObject retrieval_param = new JSONObject();
			retrieval_param.put("metric_type", "L2");
			retrieval_param.put("ncentroids", 2048);
			retrieval_param.put("nsubvector", 32);
			engine.put("retrieval_param", retrieval_param);

			tableMeta.put("engine", engine);
		}
		JSONObject properties = new JSONObject();
		Map<String, EFField> writefields = instanceConfig.getWriteFields();
		for (Map.Entry<String, EFField> entry : writefields.entrySet()) {
			if (entry.getValue().getDsl() != null) {
				properties.put(entry.getKey(), JSONObject.parse(entry.getValue().getDsl()));
			} else {
				JSONObject fields = new JSONObject();
				fields.put("type", entry.getValue().getIndextype());
				if (entry.getValue().getIndexed().equals("true")) {
					fields.put("index", true);
				}
				properties.put(entry.getValue().getAlias(), fields);
			}
		}
		tableMeta.put("properties", properties);
		return tableMeta;

	}

}
