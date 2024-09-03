package org.elasticflow.connection.sockets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.model.EFHttpResponse;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFHttpClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Vector retrieval system vearch Writer Manager
 * 
 * @author chengwen
 * @version 1.0
 * @date 2021-07-12 14:02
 */
public class VearchConnector {

	final String MASTER;

	final String ROOTER;

	protected String dbName;

	protected CloseableHttpClient httpClient = HttpClients.createDefault();

	protected JSONObject dbObject;

	protected String method = "http://";

	final String listDb = "/list/db";

	final String createDb = "/db/_create";

	private final static Logger log = LoggerFactory.getLogger("VearchConnector");

	public VearchConnector(String master, String rooter, String dbName) {
		this.MASTER = master;
		this.ROOTER = rooter;
		this.dbName = dbName;
		this.dbObject = new JSONObject();
		this.dbObject.put("name", this.dbName);
	}
	
	public String getHosts() {
		return this.MASTER+","+this.ROOTER;
	}
	
	public JSONArray getClusterStats() {
		try {
			EFHttpResponse response = EFHttpClientUtil.process(
					this.method + this.MASTER + "/_cluster/stats", HttpGet.METHOD_NAME,
					EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
			if(response.isSuccess()) {
				return JSONObject.parseArray(response.getPayload());		
			}else {
				log.error("vearch query stats exception, {}",response.getInfo());
			}				 
		} catch (Exception e) {
			log.warn("vearch query stats exception", e);
		}
		return null;
	}
	
	public JSONObject getSpaceInfo(String space) {
		try {
			EFHttpResponse response = EFHttpClientUtil.process(
					this.method + this.MASTER + "/space/" + this.dbName+"/"+space, HttpGet.METHOD_NAME,
					EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
			if(response.isSuccess()) {
				return JSONObject.parseObject(response.getPayload());
			}else {
				log.error("vearch get space {} information exception,{}",space,response.getInfo());
			}			
		} catch (Exception e) {
			log.warn("vearch get space {} information exception",space, e);
		}
		return null;
	}
	 
	public JSONObject getSpaceStats(String space) {
		try {
			EFHttpResponse response = EFHttpClientUtil.process(
					this.method + this.MASTER + "/_cluster/health", HttpGet.METHOD_NAME,
					EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
			if(response.isSuccess()) {
				JSONArray jarr = JSONArray.parseArray(response.getPayload());
				jarr = jarr.getJSONObject(0).getJSONArray("spaces");
				for (int i = 0; i < jarr.size(); i++) {
		            JSONObject jsonObject = jarr.getJSONObject(i);
		            if(jsonObject.get("name").equals(space))
		            	return jsonObject;
				}		            
			}else {
				log.error("vearch get space {} status Exception,{}",space,response.getInfo());
			}			
		} catch (Exception e) {
			log.warn("vearch get space {} status Exception",space, e);
		}
		return null;
	}
	
	public JSONObject getAllStatus(String space) {
		JSONObject res = new JSONObject();
		res.put("_cluster", getClusterStats());
		res.put("_table", getSpaceInfo(space));
		res.put("_table_stats", getSpaceStats(space));
		return res;
	}

	public boolean deleteSpace(String space) {
		try {
			EFHttpResponse response = EFHttpClientUtil.process(
					this.method + this.MASTER + "/space/" + this.dbName + "/" + space, HttpDelete.METHOD_NAME,
					EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
			if(response.isSuccess()) {
				JSONObject jr = JSONObject.parseObject(response.getPayload());
				if (Integer.valueOf(String.valueOf(jr.get("code"))) == 200) {
					return true;
				} else {
					log.warn("vearch delete space {} exception, {}",space,jr.get("msg"));
				}
			}else {
				log.error("vearch delete space {} exception, {}",space,response.getInfo());
			}			
		} catch (Exception e) {
			log.warn("vearch delete space {} exception",space, e);
		}
		return false;
	}

	public boolean checkSpaceExists(String table) {
		try {
			EFHttpResponse response = EFHttpClientUtil.process(this.method + this.MASTER + "/list/space?db=" + this.dbName,
					HttpGet.METHOD_NAME, EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
			if(response.isSuccess()) {
				JSONObject jr = JSONObject.parseObject(response.getPayload());
				if (Integer.valueOf(String.valueOf(jr.get("code"))) == 200) {
					JSONArray jArray = jr.getJSONArray("data");
					for (int i = 0; i < jArray.size(); i++) {
						if (jArray.getJSONObject(i).get("name").equals(table))
							return true;
					}
				}
			}else {
				log.error("vearch check space {} exists exception, {}",table,response.getInfo());
			}			
		} catch (Exception e) {
			log.error("vearch check space {} exists exception",table, e);
		}
		return false;
	}

	public boolean createSpace(JSONObject tableMeta) throws EFException {
		this.createDbifNotExists();
		try {
			EFHttpResponse response = EFHttpClientUtil.process(this.method + this.MASTER + "/space/" + this.dbName + "/_create",
					tableMeta.toString(), HttpPut.METHOD_NAME, EFHttpClientUtil.DEFAULT_CONTENT_TYPE, 3000,true);
			if(response.isSuccess()) {
				JSONObject jr = JSONObject.parseObject(response.getPayload());
				if (Integer.valueOf(String.valueOf(jr.get("code"))) == 200)
					return true;
				else if (Integer.valueOf(String.valueOf(jr.get("code"))) == 564) {
					log.warn("vearch create space warn,space exists!");
					return true;
				} else {
					throw new EFException("vearch create space "+this.dbName+" exception," + jr.get("msg"));
				}
			}else {
				throw new EFException(response.getInfo(),ELEVEL.Dispose);
			}			
		} catch (Exception e) {
			log.error("vearch create space {} exception",this.dbName, e);
			throw new EFException(e);
		}
	}

	public void writeSingle(String table, JSONObject datas) throws Exception {
		EFHttpResponse response = EFHttpClientUtil.process(this.method + this.ROOTER + "/" + this.dbName + "/" + table,
				datas.toString());
		if(response.isSuccess()) {
			JSONObject jr = JSONObject.parseObject(response.getPayload());
			if (Integer.valueOf(String.valueOf(jr.get("status"))) == 200)
				return;
			else {
				if(GlobalParam.DEBUG)
					log.warn("vearch {}  write data:{}",table,datas.toString());
				throw new EFException("Vearch error writing data," + jr.get("error"));
			}
		}else {
			throw new EFException(response.getInfo(),ELEVEL.Dispose);
		} 
	}

	public void deleteBatch(String table, CopyOnWriteArrayList<Object> datas) {
		try {
			int i = 0;
			while (i < datas.size()) {
				datas.get(i);
				EFHttpClientUtil.process(
						this.method + this.ROOTER + "/" + this.dbName + "/" + table + "/" + datas.get(i),
						HttpDelete.METHOD_NAME, EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
				i += 2;
			}

		} catch (Exception e) {
			log.error("vearch {} delete batch data exception",table, e);
		}
	}
	

	public JSONObject search(String table,String query,boolean feature_search) throws Exception { 
		String search_method = "/_search";
		if(feature_search==false)  
			search_method = "/_query_byids"; 
		EFHttpResponse response = EFHttpClientUtil.process(this.method + this.ROOTER + "/" + this.dbName + "/" + table + search_method,
				HttpPost.METHOD_NAME,
				EFHttpClientUtil.DEFAULT_CONTENT_TYPE,
				query);
		if(response.isSuccess()) {
			if(feature_search) {
				return JSONObject.parseObject(response.getPayload());
			}else {
				JSONArray JA = JSONObject.parseArray(response.getPayload());
				JSONObject res = new JSONObject();
				res.put("total", JA.size());
				res.put("hits", JA);
				JSONObject tmp = new JSONObject();
				tmp.put("hits", res);
				return tmp;
			} 
		}else {
			throw new EFException(response.getInfo(),ELEVEL.Dispose);
		}		
	}

	public void writeBatch(String table, CopyOnWriteArrayList<Object> datas) throws Exception {
		HttpPost rooter_post = new HttpPost(this.method + this.ROOTER + "/" + this.dbName + "/" + table + "/_bulk");
		rooter_post.addHeader("Content-Type", "application/json;charset=UTF-8");
		StringBuffer dt = new StringBuffer();
		int i = 0;
		if(datas.size()%2!=0) {
			throw new EFException("dirty data exception.");
		}
		while (i < datas.size()) {
			dt.append("\n" + String.valueOf(datas.get(i)) + "\n");
			dt.append(datas.get(i + 1).toString());
			i += 2;
		}
		StringEntity params = new StringEntity(dt.toString().trim());
		params.setContentEncoding("UTF-8");
		rooter_post.setEntity(params);
		HttpResponse response = this.httpClient.execute(rooter_post);
		String str = "";
		StringBuffer sb = new StringBuffer();
		BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
		while ((str=rd.readLine())!=null) {
			sb.append(str);
        }
		JSONArray ja;
		if(sb.substring(0, 1).equals("[")) {
			ja = JSONArray.parseArray(sb.toString());
		}else {
			ja = JSONArray.parseArray("["+sb.toString()+"]");
		}
		
		for (int j = 0; j < ja.size(); j++) {
			JSONObject jo = JSONObject.parseObject(ja.getString(j));
			if (jo.containsKey("status")==false || Integer.valueOf(String.valueOf(jo.get("status"))) != 200) {
				if(GlobalParam.DEBUG)
					log.warn("vearch {} write data:{}",table,dt.toString());
				throw new EFException("Vearch write data to "+ table +" exception," + jo.toString());
			}				
		}
	}

	public boolean close() {
		return true;
	}

	private void createDbifNotExists() {
		try {
			EFHttpResponse response = EFHttpClientUtil.process(this.method + this.MASTER + listDb, HttpGet.METHOD_NAME,
					EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
			if(response.isSuccess()) {
				JSONObject jr = JSONObject.parseObject(response.getPayload()); 
				JSONArray jsonArr = JSONArray.parseArray(jr.getString("data"));
				@SuppressWarnings("unchecked")
				Iterator<Object> it = jsonArr.iterator();
				while (it.hasNext()) {
					JSONObject jsonObj = (JSONObject) it.next();
					if (jsonObj.get("name").equals(this.dbName)) {
						return;
					}
				}
				HttpPut master_post = new HttpPut(this.method + this.MASTER + createDb);
				master_post.addHeader("Content-Type", "application/json;charset=UTF-8");
				StringEntity stringEntity = new StringEntity(this.dbObject.toString(), "UTF-8");
				stringEntity.setContentEncoding("UTF-8");
				master_post.setEntity(stringEntity);
				this.httpClient.execute(master_post);
			} else {
				log.error("vearch get space list exception {}",response.getInfo());
			}  
		} catch (IOException e) {
			log.error("vearch create space exception", e);
			return;
		}
	}
}
