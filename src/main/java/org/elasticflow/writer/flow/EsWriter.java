package org.elasticflow.writer.flow;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.connection.EsConnector;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.EFException.ETYPE;
import org.elasticflow.writer.WriterFlowSocket;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * ElasticSearch Writer Manager
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-30 14:02
 */
@NotThreadSafe
public class EsWriter extends WriterFlowSocket {

	protected EsConnector CONNS;

	private boolean reconn = false;

	private final static Logger log = LoggerFactory.getLogger("ESFlow");

	public static EsWriter getInstance(ConnectParams connectParams) {
		EsWriter o = new EsWriter();
		o.INIT(connectParams);
		return o;
	}

	@Override
	public void write(InstanceConfig instanceConfig,PipeDataUnit unit,String instance,
			String storeId, boolean isUpdate) throws EFException {
		String name = Common.getStoreName(instance, storeId);
		String type = instance;
		Map<String, EFField> transParams = instanceConfig.getWriteFields();
		WriterParam writerParam = instanceConfig.getWriterParams();
		if (unit == null || unit.getData().size() == 0) {
			log.info(instance + " WriteUnit contain Dirty data!");
			return;
		}
		if (writerParam.getKeyType().equals("scan")) {
			this.updateByScan(writerParam, unit, transParams, name, type, storeId, isUpdate);
		} else {
			this.updateByKey(unit, transParams, name, type, storeId, isUpdate);
		}
	}

	private void updateByScan(WriterParam writerParam, PipeDataUnit unit, Map<String, EFField> transParams,
			String instance, String alias, String storeId, boolean isUpdate) throws EFException {
		Script script = null;
		StringBuilder sf = new StringBuilder();
		for (Entry<String, Object> r : unit.getData().entrySet()) {
			String field = r.getKey();
			if (r.getValue() == null)
				continue;
			EFField transParam = transParams.get(field);
			if (transParam == null)
				continue;
			String value = String.valueOf(r.getValue());
			sf.append("ctx._source." + transParam.getAlias() + " = " + value + ",");
		}
		script = new Script(sf.append(GlobalParam.DEFAULT_FIELD + " = " + unit.getUpdateTime()).toString());
		try {
			UpdateByQueryRequest _UR = new UpdateByQueryRequest(instance);
			_UR.setConflicts("proceed");
			_UR.setScript(script).setQuery(QueryBuilders.termQuery(writerParam.getWriteKey(), unit.getReaderKeyVal()));
			_UR.setRefresh(true);
			getESC().getClient().updateByQuery(_UR, RequestOptions.DEFAULT);
		} catch (Exception e) {
			throw new EFException(e);
		}
	}

	private void updateByKey(PipeDataUnit unit, Map<String, EFField> transParams, String instance, String alias,
			String storeId, boolean isUpdate) throws EFException {
		try {
			XContentBuilder cbuilder = jsonBuilder().startObject();
			StringBuilder routing = new StringBuilder();
			for (Entry<String, Object> r : unit.getData().entrySet()) {
				String field = r.getKey();
				if (r.getValue() == null)
					continue;
				Object value = r.getValue();
				EFField transParam = transParams.get(field);
				if (transParam == null)
					continue;
				
				if (transParam.getAnalyzer().length() == 0) {
					if (transParam.getIndextype().equalsIgnoreCase("geo_point")) {
						String[] vs = String.valueOf(value).split(transParam.getSeparator());
						if (vs.length == 2)
							cbuilder.latlon(field, Double.parseDouble(vs[0]), Double.parseDouble(vs[1]));					
					} else if (transParam.getIndextype().equals("nested")) {
						cbuilder.array(transParam.getAlias(), JSONArray.fromObject(value));
					} else {
						cbuilder.field(transParam.getAlias(), value);		
					}						
				} else {
					cbuilder.field(transParam.getAlias(), value);
				}
				if (transParam.isRouter()) {
					routing.append(value);
				}
			}
			cbuilder.field(GlobalParam.DEFAULT_FIELD, unit.getUpdateTime());
			cbuilder.endObject();

			String id = unit.getReaderKeyVal();
			if (isUpdate) {
				UpdateRequest _UR = new UpdateRequest(instance, id);
				_UR.doc(cbuilder).upsert(cbuilder);
				if (routing.length() > 0)
					_UR.routing(routing.toString());
				if (this.isBatch) {
					getESC().getBulkProcessor().add(_UR);
				} else {
					getESC().getClient().update(_UR, RequestOptions.DEFAULT);
				}
			} else {
				IndexRequest _IR = new IndexRequest(instance).id(id);
				_IR.source(cbuilder);
				if (routing.length() > 0)
					_IR.routing(routing.toString());
				if (this.isBatch) {
					getESC().getBulkProcessor().add(_IR);
				} else {
					getESC().getClient().index(_IR, RequestOptions.DEFAULT);
				}
			}
		} catch (Exception e) {
			log.error("write Exception", e);
			if (Common.exceptionCheckContain(e, "IndexNotFoundException")) {
				throw new EFException("storeId not found", ELEVEL.Termination, ETYPE.WRITE_POS_NOT_FOUND);
			} else {
				throw new EFException(e,ELEVEL.Dispose);
			}
		}
	}

	@Override
	public void delete(String instance, String storeId, String keyColumn, String keyVal) throws EFException {
		String name = Common.getStoreName(instance, storeId);
		try {
			getESC().getClient().delete(new DeleteRequest(name, keyVal), RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new EFException(e);
		}
	}

	@Override
	public void flush() throws EFException {
		if (this.isBatch) {
			try {
				getESC().getBulkProcessor().flush();
			} catch (Exception e) {
				getESC().setBulkProcessor(null);
				throw new EFException(e);
			} 
			if (getESC().getRunState() == false) {
				getESC().setRunState(true);
				throw new EFException(getESC().getInfos());
			}
		}
	}

	@Override
	public boolean create(String instance, String storeId, InstanceConfig instanceConfig) throws EFException{
		String iName = Common.getStoreName(instance, storeId);
		try {
			log.info("create Instance " + iName);
			if (!this.storePositionExists(iName)) {
				CreateIndexRequest _CIR = new CreateIndexRequest(iName);
				_CIR.settings(Settings.builder()
						.put("index.number_of_shards",
								Integer.parseInt(instanceConfig.getExternConfigs().get("number_of_shards")))
						.put("index.number_of_replicas",
								Integer.parseInt(instanceConfig.getExternConfigs().get("number_of_replicas")))); 
				_CIR.mapping(this.getSettingMap(instanceConfig));
				CreateIndexResponse createIndexResponse = getESC().getClient().indices().create(_CIR,
						RequestOptions.DEFAULT);
				log.info("create new Instance " + iName + " response isAcknowledged:"
						+ createIndexResponse.isAcknowledged());
			}
			return true;
		} catch (Exception e) {
			reconn = true;
			throw new EFException(e,ELEVEL.Termination,ETYPE.RESOURCE_ERROR);			
		}
	}

	@Override
	public void optimize(String instance, String storeId) {
		String name = Common.getStoreName(instance, storeId);
		try {
			ForceMergeRequest request = new ForceMergeRequest(name);
			request.maxNumSegments(2);
			request.flush(true);
			ForceMergeResponse response = getESC().getClient().indices().forcemerge(request, RequestOptions.DEFAULT);

			int failed_cnt = response.getFailedShards();
			if (failed_cnt > 0) {
				log.warn("Instance " + name + " optimize getFailedShards " + failed_cnt);
			} else {
				log.info("Instance " + name + " optimize Success!");
			}
		} catch (Exception e) {
			log.error("Instance " + name + " optimize failed.", e);
		}
	}

	@Override
	public void removeInstance(String instance, String storeId) {
		if (storeId == null || storeId.length() == 0)
			storeId = "a";
		String iName = Common.getStoreName(instance, storeId);
		try {
			log.info("trying to remove Instance " + iName);
			GetIndexRequest _GIR = new GetIndexRequest(iName);
			boolean exists = getESC().getClient().indices().exists(_GIR, RequestOptions.DEFAULT);
			if (!exists) {
				log.info("Instance " + iName + " didn't exist.");
			} else {
				DeleteIndexRequest _DIR = new DeleteIndexRequest(iName);
				AcknowledgedResponse deleteResponse = getESC().getClient().indices().delete(_DIR,
						RequestOptions.DEFAULT);
				if (deleteResponse.isAcknowledged()) {
					log.info("Instance " + iName + " removed ");
				}
			}
		} catch (Exception e) {
			log.error("remove Instance " + iName + " Exception", e);
		}
	}

	@Override
	public void setAlias(String instanceName, String storeId, String aliasName) {
		String iName = Common.getStoreName(instanceName, storeId);
		try {
			log.info("trying to setting Alias " + aliasName + " to index " + iName);
			IndicesAliasesRequest _IAR = new IndicesAliasesRequest();
			AliasActions aliasAction = new AliasActions(AliasActions.Type.ADD).index(iName).alias(aliasName);
			_IAR.addAliasAction(aliasAction);
			AcknowledgedResponse response = getESC().getClient().indices().updateAliases(_IAR, RequestOptions.DEFAULT);
			if (response.isAcknowledged()) {
				log.info("alias " + aliasName + " setted to " + iName);
			}
		} catch (Exception e) {
			log.error("alias " + aliasName + " set to " + iName + " Exception.", e);
		}
	}

	private Map<String, Object> JsonToMap(JSONObject j) {
		Map<String, Object> map = new HashMap<>();
		Iterator<?> iterator = j.keys();
		while (iterator.hasNext()) {
			String key = (String) iterator.next();
			Object value = j.get(key);
			map.put(key, value);
		}
		return map;
	}

	private Map<String, Object> getSettingMap(InstanceConfig instanceConfig) {
		Map<String, Object> settingMap = new HashMap<String, Object>();
		Map<String, Object> root_map = new HashMap<String, Object>();
		try {
			Map<String, Object> map = new HashMap<String, Object>();
			for (Map.Entry<String, EFField> e : instanceConfig.getWriteFields().entrySet()) {
				map = new HashMap<String, Object>();
				EFField p = e.getValue();
				if (p.getName() == null)
					continue;
				map.put("type", p.getIndextype());
				if (p.getStored().toLowerCase().equals("true")) {
					map.put("store", true);
				}
				if (p.getDsl() != null) {
					map.putAll(JsonToMap(JSONObject.fromObject(p.getDsl())));
				}
				if (p.getIndexed().toLowerCase().equals("true")) {
					if (p.getAnalyzer().length() > 0) {
						map.put("analyzer", p.getAnalyzer());
					}
				} 
				settingMap.put(p.getAlias(), map);
			}
			settingMap.put(GlobalParam.DEFAULT_FIELD, new HashMap<String, Object>() {
				private static final long serialVersionUID = 1L;
				{
					put("type", "long");
				}
			});
			root_map.put("properties", settingMap);
		} catch (Exception e) {
			log.error("getSettingMap Exception", e);
		}  
		return root_map;
	}

	protected String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) throws EFException {
		boolean a_alias = false;
		boolean b_alias = false;
		String select = "";
		try {
			boolean a = this.storePositionExists(Common.getStoreName(mainName, "a"));
			if (a)
				a_alias = getIndexAlias(mainName, "a", instanceConfig.getAlias());
			boolean b = this.storePositionExists(Common.getStoreName(mainName, "b"));
			if (b)
				b_alias = getIndexAlias(mainName, "b", instanceConfig.getAlias());
			if (isIncrement) {
				if (a && b) {
					if (a_alias) {
						if (b_alias) {
							if (getDocumentNums(mainName, "a") > getDocumentNums(mainName, "b")) {
								select = "a";
							} else {
								select = "b";
							}
						} else {
							select = "a";
						}
					} else {
						select = "b";
					}
				} else {
					select = a ? "a" : (b ? "b" : "a");
				}

				if ((select.equals("a") && !a) || (select.equals("b") && !b)) {
					this.create(mainName, select, instanceConfig);
				}

				if ((select.equals("a") && !a) || (select.equals("b") && !b)
						|| !this.getIndexAlias(mainName, select, instanceConfig.getAlias())) {
					setAlias(mainName, select, instanceConfig.getAlias());
				}
			} else {
				if (a && b) {
					if (a_alias) {
						if (b_alias) {
							if (getDocumentNums(mainName, "a") > getDocumentNums(mainName, "b")) {
								getESC().getClient().indices().delete(
										new DeleteIndexRequest(Common.getStoreName(mainName, "b")),
										RequestOptions.DEFAULT);
								select = "b";
							} else {
								getESC().getClient().indices().delete(
										new DeleteIndexRequest(Common.getStoreName(mainName, "a")),
										RequestOptions.DEFAULT);
								select = "a";
							}
						} else {
							select = "b";
						}
					}
					select = "a";
				} else {
					select = "b";
					if (b && b_alias) {
						select = "a";
					}
				}
			}
		} catch (Exception e) {
			throw new EFException(e,ELEVEL.Termination,ETYPE.RESOURCE_ERROR);	
		}
		return select;
	}

	private long getDocumentNums(String instance, String storeId) throws IOException {
		String iName = Common.getStoreName(instance, storeId);
		CountRequest countRequest = new CountRequest(iName);
		CountResponse response = getESC().getClient().count(countRequest, RequestOptions.DEFAULT);
		return response.getCount();
	}

	private boolean getIndexAlias(String instanceName, String storeId, String alias) throws IOException {
		String iName = Common.getStoreName(instanceName, storeId);
		GetIndexRequest request = new GetIndexRequest(iName);
		boolean exists = getESC().getClient().indices().exists(request, RequestOptions.DEFAULT);
		return exists;
	}

	@Override
	public boolean storePositionExists(String storeName) {
		try {
			return getESC().getClient().indices().exists(new GetIndexRequest(storeName),
					RequestOptions.DEFAULT);
		} catch (IOException e) {
			log.error("store Position check IOException", e);
		}
		return false;
	}
	
	private synchronized EsConnector getESC() {
		if (this.CONNS == null || reconn) {
			reconn = false;
			this.CONNS = (EsConnector) GETSOCKET().getConnection(END_TYPE.writer);
		}
		return this.CONNS;
	}

}
