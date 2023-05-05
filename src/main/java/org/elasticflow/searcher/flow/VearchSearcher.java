package org.elasticflow.searcher.flow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.connection.VearchConnector;
import org.elasticflow.connection.handler.ConnectionHandler;
import org.elasticflow.model.searcher.ResponseDataUnit;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.searcher.SearcherFlowSocket;
import org.elasticflow.searcher.handler.SearcherHandler;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class VearchSearcher extends SearcherFlowSocket{
		
	private ConnectionHandler handler;
	
	private final static Logger log = LoggerFactory.getLogger(VearchSearcher.class);

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
		if(connectParams.getWhp().getHandler()!=null){ 
			try {
				this.handler = (ConnectionHandler)Class.forName(connectParams.getWhp().getHandler()).getDeclaredConstructor().newInstance();
				this.handler.init(connectParams);
			} catch (Exception e) {
				log.error("Init handler Exception",e);
			}
		} 
	}  
	
	@Override
	public SearcherResult Search(SearcherModel<?, ?, ?> query, String instance, SearcherHandler handler)
			throws EFException {
		SearcherResult res = new SearcherResult(); 
		PREPARE(false, true, false);
		boolean releaseConn = false;
		if(!ISLINK())
			return res;
		try {
			String table = Common.getStoreName(instance, query.getStoreId());
			VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.searcher);  
			JSONObject qu = new JSONObject();
			qu.put("size", query.getCount());
			qu.put("query", JSONObject.parse(query.getFq()));		
			if(query.getFl()!=null)
				qu.put("fields", query.getFl().split(","));
			JSONObject JO = conn.search(table, qu.toJSONString());
			if (query.isShowQueryInfo()) {
				res.setQueryDetail(qu.toString()); 
			}
			if(JO.getJSONObject("hits").containsKey("total")) {
				List<ResponseDataUnit> unitSet = new ArrayList<ResponseDataUnit>();
				int total = JO.getJSONObject("hits").getIntValue("total");
				if(total>0) {
					JSONArray hits = JO.getJSONObject("hits").getJSONArray("hits");
					for(int i=0;i<hits.size();i++) {
						ResponseDataUnit rn = ResponseDataUnit.getInstance();
						JSONObject row = hits.getJSONObject(i).getJSONObject("_source"); 
						Iterator<Entry<String, Object>> it = row.entrySet().iterator();
						while (it.hasNext()) {
							Entry<String, Object> entry = it.next();
							rn.addObject(entry.getKey(), entry.getValue());
				        } 
						rn.addObject("_score", hits.getJSONObject(i).get("_score"));
						unitSet.add(rn);
					}
				}
				if(query.isShowStats()) {
					res.setStat(conn.getAllStatus(table));
				}
				res.setTotalHit(total);
				res.setUnitSet(unitSet);
			}else {
				res.setSuccess(false);
				res.setErrorInfo("please check the search parameters!");
			} 
		}catch(Exception e){
			releaseConn = true; 
			throw Common.getException(e);
		}finally{
			REALEASE(false,releaseConn);
		} 
		return res;
	}

}
