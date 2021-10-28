package org.elasticflow.searcher.flow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class VearchSearcher extends SearcherFlowSocket{
		
	private ConnectionHandler handler;
	
	private final static Logger log = LoggerFactory.getLogger(VearchSearcher.class);

	public static VearchSearcher getInstance(ConnectParams connectParams) {
		VearchSearcher o = new VearchSearcher();
		o.INIT(connectParams);
		return o;
	}
	
	@Override
	public void INIT(ConnectParams connectParams) {
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
		PREPARE(false, true);
		boolean releaseConn = false;
		if(!ISLINK())
			return res;
		try {
			String table = Common.getStoreName(instance, query.getStoreId());
			VearchConnector conn = (VearchConnector) GETSOCKET().getConnection(END_TYPE.searcher); 
			JSONObject JO = conn.search(table, query.getFq());
			if(JO.getJSONObject("hits").containsKey("total")) {
				List<ResponseDataUnit> unitSet = new ArrayList<ResponseDataUnit>();
				int total = JO.getJSONObject("hits").getInt("total");
				if(total>0) {
					JSONArray hits = JO.getJSONObject("hits").getJSONArray("hits");
					for(int i=0;i<hits.size();i++) {
						ResponseDataUnit rn = ResponseDataUnit.getInstance();
						JSONObject row = hits.getJSONObject(i).getJSONObject("_source");
						@SuppressWarnings("unchecked")
						Iterator<String> it = row.keys();
						while (it.hasNext()) {
							String k = it.next();
							rn.addObject(k, row.get(k));
				        } 
						rn.addObject("_score", hits.getJSONObject(i).get("_score"));
						unitSet.add(rn);
					}
				}			
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
