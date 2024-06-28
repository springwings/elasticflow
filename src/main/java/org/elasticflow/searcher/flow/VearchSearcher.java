package org.elasticflow.searcher.flow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.connection.sockets.VearchConnector;
import org.elasticflow.field.EFField;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.searcher.ResponseDataUnit;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.searcher.SearcherFlowSocket;
import org.elasticflow.searcher.handler.SearcherHandler;
import org.elasticflow.searcher.parser.VearchQueryParser;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.instance.TaskUtil;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Main Run Class for Searcher
 * 
 * @author chengwen
 * @version 2.0
 * @date 2022-10-26 09:23
 */
public class VearchSearcher extends SearcherFlowSocket {  

	public static VearchSearcher getInstance(ConnectParams connectParams) {
		VearchSearcher o = new VearchSearcher();
		o.initConn(connectParams);
		return o;
	}

	@Override
	public void initConn(ConnectParams connectParams) {
		this.connectParams = connectParams;
		this.poolName = connectParams.getWhp().getPoolName(connectParams.getL1Seq());
		this.instanceConfig = connectParams.getInstanceConfig(); 
	}

	@Override
	public void Search(SearcherModel<?> searcherModel, String instance, SearcherHandler handler, EFResponse efResponse)
			throws EFException {
		SearcherResult res = new SearcherResult();
		PREPARE(false, true, false);
		boolean clearConn = false;
		if (connStatus()) {
			try {
				VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.searcher);
				VearchQueryParser VQP = new VearchQueryParser(); 
				VQP.parseQuery(instanceConfig, searcherModel);
				if (searcherModel.getFl() != null)
					VQP.getSearchObj().put("fields", searcherModel.getFl().split(","));
				String table = TaskUtil.getStoreName(instance, searcherModel.getStoreId());
				VQP.getSearchObj().put("size", searcherModel.getCount());
				if(searcherModel.getSortinfo().size()>0)
					VQP.getSearchObj().put("sort", searcherModel.getSortinfo());
				JSONObject JO = conn.search(table, VQP.getSearchObj().toJSONString(),VQP.feature_search);
				efResponse.setInstance(table);
				if (searcherModel.isShowQueryInfo()) {
					res.setQueryDetail(VQP.getSearchObj());
				}
				if (JO.containsKey("hits")) {
					List<ResponseDataUnit> unitSet = new ArrayList<ResponseDataUnit>();
					int total = JO.getJSONObject("hits").getIntValue("total");
					if (total > 0) {
						List<String> returnFields = new ArrayList<String>();
						if (searcherModel.getFl() != null) {
							for (String s : searcherModel.getFl().split(",")) {
								returnFields.add(s);
							}
						} else {
							Map<String, EFField> tmpFields = instanceConfig.getSearchFields();
							for (Map.Entry<String, EFField> e : tmpFields.entrySet()) {
								if (e.getValue().getStored().equalsIgnoreCase("true"))
									returnFields.add(e.getKey());
							}
						}

						JSONArray hits = JO.getJSONObject("hits").getJSONArray("hits");
						for (int i = 0; i < hits.size(); i++) {
							ResponseDataUnit rn = ResponseDataUnit.getInstance();
							JSONObject row = hits.getJSONObject(i).getJSONObject("_source");
							Iterator<Entry<String, Object>> it = row.entrySet().iterator();
							while (it.hasNext()) {
								Entry<String, Object> entry = it.next();
								if (returnFields.contains(entry.getKey()))
									rn.addObject(entry.getKey(), entry.getValue());
							}
							rn.addObject(GlobalParam.RESPONSE_SCORE, hits.getJSONObject(i).get("_score"));
							unitSet.add(rn);
						}
					}
					if (searcherModel.isShowStats()) {
						res.setStat(conn.getAllStatus(table));
					}
					res.setTotalHit(total);
					res.setUnitSet(unitSet);
				} else {
					res.setSuccess(false);
					res.setErrorInfo("please check the search parameters!" + JO.toJSONString());
				}
				 
			} catch (Exception e) {
				clearConn = true;
				throw Common.convertException(e);
			} finally {
				releaseConn(false, clearConn);
			}
		}  
		this.formatResult(res, efResponse);
	}

}
