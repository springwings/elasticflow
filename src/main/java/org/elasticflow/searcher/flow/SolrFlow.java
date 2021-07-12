package org.elasticflow.searcher.flow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.elasticflow.connect.handler.ConnectionHandler;
import org.elasticflow.field.EFField;
import org.elasticflow.model.searcher.ResponseDataUnit;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseNosqlParam;
import org.elasticflow.searcher.SearcherFlowSocket;
import org.elasticflow.searcher.handler.Handler;
import org.elasticflow.util.EFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:23
 */
public class SolrFlow extends SearcherFlowSocket { 
	
	private String collectionName = "";
	private long storetime = 0; 
	private ConnectionHandler handler;

	private final static Logger log = LoggerFactory.getLogger(SolrFlow.class);

	public static SolrFlow getInstance(ConnectParams connectParams) {
		SolrFlow o = new SolrFlow();
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
				this.handler = (ConnectionHandler)Class.forName(connectParams.getWhp().getHandler()).newInstance();
				this.handler.init(connectParams);
			} catch (Exception e) {
				log.error("Init handler Exception",e);
			}
		} 
	} 

	@Override
	public SearcherResult Search(SearcherModel<?, ?, ?> fq, String instance,Handler handler) throws EFException{
		SearcherResult res = new SearcherResult();
		PREPARE(false, true);
		boolean releaseConn = false;
		if(!ISLINK())
			return res;
		try {
			CloudSolrClient conn = (CloudSolrClient) GETSOCKET().getConnection(true);
			int start = fq.getStart();
			int count = fq.getCount();
			SolrQuery qb = (SolrQuery) fq.getQuery();
			qb.setParam("defType", "edismax");
			qb.setRequestHandler(fq.getRequestHandler());
			QueryResponse response = getSearchResponse(conn, qb, instance, start,
					count, fq);
			NamedList<Object> commonResponse = response.getResponse();
			if (fq.isShowQueryInfo()) {
				res.setQueryDetail(qb.toString()); 
				res.setExplainInfo(response.getExplainMap());
			}
			for (int i = 0; i < commonResponse.size(); i++) {
				String name = commonResponse.getName(i);
				Object value = commonResponse.getVal(i);
				if (name.equals("response")) {
					SolrDocumentList v = (SolrDocumentList) value;
					res.setTotalHit((int) v.getNumFound());
				}
			}
			if(handler==null) {
				addResult(res, response,fq); 
			}else {
				handler.Handle(res,response,fq);
			}
		}catch(Exception e){
			releaseConn = true; 
			throw e;
		}finally{
			REALEASE(false,releaseConn);
		} 
		return res;
	} 
	
	private String getCollection() {
		if (this.handler!=null) { 
			if(this.collectionName.length() < 1
					|| System.currentTimeMillis() / 1000 - this.storetime > 120){ 
				this.collectionName = this.handler.getData();
				this.storetime = System.currentTimeMillis() / 1000;
			}  
		}
		if(this.collectionName==null || this.collectionName=="")
				this.collectionName = ((WarehouseNosqlParam) this.connectParams.getWhp()).getDefaultValue();
		return this.collectionName;
	} 
	
	private void addResult(SearcherResult res, QueryResponse rps,SearcherModel<?, ?, ?> fq) { 
		GroupResponse groupResponse = rps.getGroupResponse();
		NamedList<Object> commonResponse = rps.getResponse();
		boolean setnum = true;
		if (groupResponse != null) {
			List<GroupCommand> groupList = groupResponse.getValues();
			for (GroupCommand groupCommand : groupList) {
				if (setnum) {
					res.setTotalHit(groupCommand.getNGroups());
				}
				setnum = false;
				List<Group> tmps = groupCommand.getValues();
				for (Group g : tmps) {
					ResponseDataUnit u = ResponseDataUnit.getInstance();
					u.addObject(g.getGroupValue(), g.getResult());
					res.getUnitSet().add(u);
				}
			}
		} else {
			for (int i = 0; i < commonResponse.size(); i++) {
				String name = commonResponse.getName(i);
				Object value = commonResponse.getVal(i);
				if (name.equals("response")) {
					SolrDocumentList v = (SolrDocumentList) value;
					if (setnum)
						res.setTotalHit((int) v.getNumFound());
					setnum = false;
					for (SolrDocument sd : v) {
						ResponseDataUnit u = ResponseDataUnit.getInstance();
						for (String n : sd.getFieldNames()) {
							if(fq.isShowQueryInfo()){ 
								if(n.equals("score")){
									u.addObject("_SCORE", sd.get(n));
									continue;
								} 
							}
							u.addObject(n, sd.get(n));
						}
						res.getUnitSet().add(u);
					}
					break;
				}
			}
		}
		if (rps.getFacetFields() != null) {
			Map<String, Map<String, Object>> fc = new HashMap<String, Map<String, Object>>();
			List<FacetField> fields = rps.getFacetFields(); 
			for (FacetField facet : fields) {
				Map<String, Object> _row = new HashMap<String, Object>();
				List<Count> counts = facet.getValues();
				for (Count c : counts) {
					_row.put(c.getName(), c.getCount()+"");
				}
				fc.put(facet.getName(), _row);
			}  
			if(rps.getFacetPivot()!=null && rps.getFacetPivot().size()>0) {
				NamedList<List<PivotField>> namedList = rps.getFacetPivot();  
				for(int i=0;i<namedList.size();i++){    
					HashMap<String, Object> _r = new HashMap<>();
					_r.put("data", namedList.getVal(i));
	                fc.put(namedList.getName(i), _r);
				}
			} 
			if(rps.getFacetQuery()!=null && rps.getFacetQuery().size()>0) {
				HashMap<String, Object> _r = new HashMap<>();
				_r.put("data", rps.getFacetQuery());
				fc.put("facet_query", _r);
			}
			res.setFacetInfo(fc);
		}
	}

	private QueryResponse getSearchResponse(CloudSolrClient conn,SolrQuery qb, String index,
			int start, int count, SearcherModel<?, ?, ?> fq) {
		conn.setDefaultCollection(getCollection()); 
		qb.setParam("wt", "json");
		qb.setRows(count);
		qb.setStart(start);
		
		if(fq.getFq().length() > 0){
			qb.setParam("fq", fq.getFq());
		}

		String fl = "";
		if (fq.getFl().length() > 0) {
			fl = fq.getFl(); 
		} else { 
			for (Map.Entry<String, EFField> e : instanceConfig
					.getWriteFields().entrySet()) {
				if (e.getValue().getStored().equalsIgnoreCase("true"))
					fl += "," + e.getKey();
			} 
			if (fl.length() > 1) {
				fl = fl.substring(1);
			}
		}
		if (fq.isShowQueryInfo()){
			qb.set("debug", true);
			qb.add("fl", fl+",score"); 
		}else{
			qb.add("fl", fl); 
		} 
		try {
			return conn.query(qb);
		} catch (Exception e) {
			log.error("getSearchResponse Exception,", e);
		}
		return null;
	} 
}
