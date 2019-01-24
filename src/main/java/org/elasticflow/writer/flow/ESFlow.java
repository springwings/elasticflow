package org.elasticflow.writer.flow;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.Mechanism;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.connect.ESConnector;
import org.elasticflow.field.RiverField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.FNException;
import org.elasticflow.util.FNException.ELEVEL;
import org.elasticflow.util.FNException.ETYPE;
import org.elasticflow.writer.WriterFlowSocket;
 
/**
 * ElasticSearch Writer Manager
 * @author chengwen
 * @version 2.0
 * @date 2018-10-30 14:02
 */
@NotThreadSafe
public class ESFlow extends WriterFlowSocket {
	
	protected ESConnector CONNS;
	
	private boolean reconn=false;
 
	private final static Logger log = LoggerFactory.getLogger("ESFlow");

	public static ESFlow getInstance(ConnectParams connectParams) {
		ESFlow o = new ESFlow();
		o.INIT(connectParams);
		return o;
	} 

	@Override
	public void write(WriterParam writerParam, PipeDataUnit unit, Map<String, RiverField> transParams, String instance,
			String storeId, boolean isUpdate) throws FNException { 
		String name = Common.getStoreName(instance, storeId);
		String type = instance;
		if (unit==null || unit.getData().size() == 0) {
			log.info(instance+" WriteUnit contain Dirty data!");
			return;
		} 
		if(writerParam.getKeyType().equals("scan")) {
			this.updateByScan(writerParam,unit, transParams, name, type, storeId, isUpdate);
		}else {
			this.updateByKey(unit, transParams, name, type, storeId, isUpdate);
		} 
	}
	
	private void updateByScan(WriterParam writerParam,PipeDataUnit unit, Map<String, RiverField> transParams, String instance,String alias,
			String storeId, boolean isUpdate) throws FNException{  
		Script script = null;
		StringBuilder sf = new StringBuilder();
		for (Entry<String, Object> r : unit.getData().entrySet()) {
			String field = r.getKey();
			if (r.getValue() == null)
				continue;
			RiverField transParam = transParams.get(field);
			if (transParam == null)
				transParam = transParams.get(field.toLowerCase());
			if (transParam == null)
				continue;
			String value = String.valueOf(r.getValue());
			sf.append("ctx._source."+transParam.getAlias()+" = "+value+","); 
		} 
		script = new Script(sf.append(GlobalParam.DEFAULT_FIELD+" = "+unit.getUpdateTime()).toString()); 
		try { 
			UpdateByQueryRequestBuilder _UR = UpdateByQueryAction.INSTANCE.newRequestBuilder(getESC().getClient());
			_UR.source(instance).script(script).filter(QueryBuilders.termQuery(writerParam.getWriteKey(),unit.getKeyColumnVal())).execute().get();
		} catch (Exception e) {
			throw new FNException(e);
		} 
	}
	
	private void updateByKey(PipeDataUnit unit, Map<String, RiverField> transParams, String instance,String alias,
			String storeId, boolean isUpdate) throws FNException{
		try { 
			XContentBuilder cbuilder = jsonBuilder().startObject();
			StringBuilder routing = new StringBuilder();
			for (Entry<String, Object> r : unit.getData().entrySet()) {
				String field = r.getKey();
				if (r.getValue() == null)
					continue;
				String value = String.valueOf(r.getValue());
				RiverField transParam = transParams.get(field);
				if (transParam == null)
					transParam = transParams.get(field.toLowerCase());
				if (transParam == null)
					continue;
				
				if (transParam.getAnalyzer().length()==0) {
					if(transParam.getIndextype().equalsIgnoreCase("geo_point")) {
						String[] vs = value.split(transParam.getSeparator());
						if(vs.length==2)
							cbuilder.latlon(field, Double.parseDouble(vs[0]), Double.parseDouble(vs[1]));
					}else if (transParam.getSeparator() != null) {
						String[] vs = value.split(transParam.getSeparator());
						cbuilder.array(transParam.getAlias(), vs);
					} else 
						cbuilder.field(transParam.getAlias(), value);
				} else {
					cbuilder.field(transParam.getAlias(), value);
				}
				if (transParam.isRouter()) {
					routing.append(value);
				}
			}
			cbuilder.field(GlobalParam.DEFAULT_FIELD, unit.getUpdateTime());
			cbuilder.endObject();
			
			String id = unit.getKeyColumnVal();
			if (isUpdate) {
				UpdateRequest _UR = new UpdateRequest(instance, alias, id);
				_UR.doc(cbuilder).upsert(cbuilder);
				if (routing.length() > 0)
					_UR.routing(routing.toString());
				if (this.isBatch) {
					getESC().getBulkProcessor().add(_UR); 
				} else {
					getESC().getClient().update(_UR).get();
				}
			} else {
				IndexRequestBuilder _IB = getESC().getClient().prepareIndex(instance, alias, id);
				_IB.setSource(cbuilder);
				if (routing.length() > 0)
					_IB.setRouting(routing.toString());
				if (this.isBatch) {  
					getESC().getBulkProcessor().add(_IB.request()); 
				} else {
					_IB.execute().actionGet();
				}
			} 
		} catch (Exception e) {
			log.error("write Exception", e); 
			if (e.getMessage().contains("IndexNotFoundException")) {
				throw new FNException("storeId not found",ELEVEL.Dispose,ETYPE.WRITE_POS_NOT_FOUND);
			} else {
				throw new FNException(e);
			}
		}
	} 

	@Override
	public void delete(String instance, String storeId,String keyColumn, String keyVal) throws FNException {
		String name = Common.getStoreName(instance, storeId);
		String type = instance;
		getESC().getClient().prepareDelete(name, type, keyVal);
	}

	@Override
	public void flush() throws Exception {
		if (this.isBatch) {
			getESC().getBulkProcessor().flush();
			if (getESC().getRunState() == false) {
				getESC().setRunState(true);
				throw new FNException("BulkProcessor Exception!Need Redo!");
			}
		}
	} 
	
	@Override
	public boolean create(String instance, String storeId, Map<String, RiverField> transParams) {
		String name = Common.getStoreName(instance, storeId);
		String type = instance;
		try {
			log.info("create Instance " + name + ":" + type); 
			IndicesExistsResponse indicesExistsResponse = getESC().getClient().admin().indices()
					.exists(new IndicesExistsRequest(name)).actionGet();
			if (!indicesExistsResponse.isExists()) {
				CreateIndexResponse createIndexResponse = getESC().getClient().admin().indices()
						.create(new CreateIndexRequest(name)).actionGet();
				log.info("create new Instance " + name + " response isAcknowledged:"
						+ createIndexResponse.isAcknowledged());
			}

			PutMappingRequest mappingRequest = new PutMappingRequest(name).type(type);
			mappingRequest.source(getSettingMap(transParams));
			PutMappingResponse response = getESC().getClient().admin().indices().putMapping(mappingRequest).actionGet();
			log.info("setting response isAcknowledged:" + response.isAcknowledged());
			return true;
		} catch (Exception e) {
			reconn = true;
			log.error("setting Instance " + name + ":" + type + " failed.", e);
			return false;
		}
	}

	@Override
	public void optimize(String instance, String storeId) {
		String name = Common.getStoreName(instance, storeId);
		try {
			ForceMergeRequest request = new ForceMergeRequest(name);
			request.maxNumSegments(2);
			request.flush(true);
			request.onlyExpungeDeletes(true);

			ForceMergeResponse response = getESC().getClient().admin().indices().forceMerge(request).actionGet();
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
		String name = Common.getStoreName(instance, storeId);
		try {
			log.info("trying to remove Instance " + name);
			IndicesExistsResponse res = getESC().getClient().admin().indices().prepareExists(name).execute()
					.actionGet();
			if (!res.isExists()) {
				log.info("Instance " + name + " didn't exist.");
			} else {
				DeleteIndexRequest deleteRequest = new DeleteIndexRequest(name);
				DeleteIndexResponse deleteResponse = getESC().getClient().admin().indices().delete(deleteRequest)
						.actionGet();
				if (deleteResponse.isAcknowledged()) {
					log.info("Instance " + name + " removed ");
				}
			}
		} catch (Exception e) {
			log.error("remove Instance " + name + " Exception", e);
		}
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
	public void setAlias(String instanceName, String storeId, String aliasName) {
		String name = Common.getStoreName(instanceName, storeId);
		try {
			log.info("trying to setting Alias " + aliasName + " to index " + name);
			IndicesAliasesResponse response = getESC().getClient().admin().indices().prepareAliases().addAlias(name,
					aliasName).execute().actionGet();
			if (response.isAcknowledged()) {
				log.info("alias " + aliasName + " setted to " + name);
			}
		} catch (Exception e) {
			log.error("alias " + aliasName + " set to " + name + " Exception.", e);
		}
	} 
	
	private Map<String, Object> getSettingMap(Map<String, RiverField> transParams) {
		Map<String, Object> settingMap = new HashMap<String, Object>();
		Map<String, Object> root_map = new HashMap<String, Object>();
		try { 
			for (Map.Entry<String, RiverField> e : transParams.entrySet()) {
				Map<String, Object> map = new HashMap<String, Object>();
				RiverField p = e.getValue();
				if (p.getName() == null)
					continue;
				map.put("type", p.getIndextype()); // type is must
				if (p.getStored().toLowerCase().equals("false")) {
					map.put("store", false);
				}else {
					map.put("store", true);
				}
				if(p.getDsl()!=null) {
					for(String r:p.getDsl().split(",")) {
						String[] _dsl = r.split(":");
						map.put(_dsl[0], _dsl[1]);
					}
				}
				if (p.getIndexed().toLowerCase().equals("true")) { 
					if (p.getAnalyzer().length()>0) {
						map.put("analyzer", p.getAnalyzer());
					}
				} else {
					//map.put("analyzed", false);
				} 
				settingMap.put(p.getAlias(), map);
			}
			settingMap.put(GlobalParam.DEFAULT_FIELD, new HashMap<String, Object>(){
				private static final long serialVersionUID = 1L;{put("type", "long");}});
			root_map.put("properties", settingMap);
		} catch (Exception e) {
			log.error("getSettingMap Exception",e);
		}   
		Map<String, Object> _source_map = new HashMap<String, Object>();
		_source_map.put("enabled", "true");
		root_map.put("_source", _source_map);
		Map<String, Object> _all_map = new HashMap<String, Object>();
		_all_map.put("enabled", "false");
		root_map.put("_all", _all_map);
		return root_map;
	}
    
	private String timeMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		long current=System.currentTimeMillis(); 
		return String.valueOf(current/(1000*3600*24)*(1000*3600*24)-TimeZone.getDefault().getRawOffset()); 
	} 
 
	private String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		boolean a_alias = false;
		boolean b_alias = false;
		boolean a = getESC().getClient().admin().indices()
				.exists(new IndicesExistsRequest(Common.getStoreName(mainName, "a"))).actionGet().isExists();
		if (a)
			a_alias = getIndexAlias(mainName, "a", instanceConfig.getAlias());
		boolean b = getESC().getClient().admin().indices()
				.exists(new IndicesExistsRequest(Common.getStoreName(mainName, "b"))).actionGet().isExists();
		if (b)
			b_alias = getIndexAlias(mainName, "b", instanceConfig.getAlias());
		String select = "";
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
				this.create(mainName, select, instanceConfig.getWriteFields());
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
							getESC().getClient().admin().indices()
							.delete(new DeleteIndexRequest(Common.getStoreName(mainName, "b")));
							select = "b";
						} else {  
							getESC().getClient().admin().indices()
							.delete(new DeleteIndexRequest(Common.getStoreName(mainName, "a")));
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
		return select;
	}
	
	private long getDocumentNums(String instance, String storeId) {
		String name = Common.getStoreName(instance, storeId);
		IndicesStatsResponse response = getESC().getClient().admin().indices().prepareStats(name).all().get();
		long res = response.getPrimaries().getDocs().getCount();
		return res;
	}

	private boolean getIndexAlias(String instanceName, String storeId, String alias) {
		String name = Common.getStoreName(instanceName, storeId);
		AliasesExistResponse response = getESC().getClient().admin().indices().prepareAliasesExist(alias)
				.setIndices(name).get();
		return response.exists();
	} 
	
	private synchronized ESConnector getESC() { 
		if(this.CONNS==null || reconn) {
			reconn = false;
			this.CONNS = (ESConnector) GETSOCKET().getConnection(false); 
		} 
		return this.CONNS;
	}
}
