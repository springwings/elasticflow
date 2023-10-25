package org.elasticflow.connection;

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
	
	public JSONArray getClusterStats() {
		try {
			String response = EFHttpClientUtil.process(
					this.method + this.MASTER + "/_cluster/stats", HttpGet.METHOD_NAME,
					EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
			return JSONObject.parseArray(response);			 
		} catch (Exception e) {
			log.warn("query stats Exception", e);
		}
		return null;
	}
	
	public JSONObject getSpaceInfo(String space) {
		try {
			String response = EFHttpClientUtil.process(
					this.method + this.MASTER + "/space/" + this.dbName+"/"+space, HttpGet.METHOD_NAME,
					EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
			return JSONObject.parseObject(response);
		} catch (Exception e) {
			log.warn("get space info Exception", e);
		}
		return null;
	}
	
	public JSONObject getAllStatus(String space) {
		JSONObject res = new JSONObject();
		res.put("_cluster", getClusterStats());
		res.put("_table", getSpaceInfo(space));
		return res;
	}

	public boolean deleteSpace(String space) {
		try {
			String response = EFHttpClientUtil.process(
					this.method + this.MASTER + "/space/" + this.dbName + "/" + space, HttpDelete.METHOD_NAME,
					EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
			JSONObject jr = JSONObject.parseObject(response);
			if (Integer.valueOf(String.valueOf(jr.get("code"))) == 200) {
				return true;
			} else {
				log.warn("delete Space {} Exception,",space,jr.get("msg"));
			}
		} catch (Exception e) {
			log.warn("delete Space {} Exception",space, e);
		}
		return false;
	}

	public boolean checkSpaceExists(String table) {
		try {
			String response = EFHttpClientUtil.process(this.method + this.MASTER + "/list/space?db=" + this.dbName,
					HttpGet.METHOD_NAME, EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
			JSONObject jr = JSONObject.parseObject(response);
			if (Integer.valueOf(String.valueOf(jr.get("code"))) == 200) {
				JSONArray jArray = jr.getJSONArray("data");
				for (int i = 0; i < jArray.size(); i++) {
					if (jArray.getJSONObject(i).get("name").equals(table))
						return true;
				}
			}
		} catch (Exception e) {
			log.error("check Space {} Exists Exception",table, e);
		}
		return false;
	}

	public boolean createSpace(JSONObject tableMeta) throws EFException {
		this.createDbifNotExists();
		try {
			String response = EFHttpClientUtil.process(this.method + this.MASTER + "/space/" + this.dbName + "/_create",
					tableMeta.toString(), HttpPut.METHOD_NAME, EFHttpClientUtil.DEFAULT_CONTENT_TYPE, 3000,true);
			JSONObject jr = JSONObject.parseObject(response);
			if (Integer.valueOf(String.valueOf(jr.get("code"))) == 200)
				return true;
			else if (Integer.valueOf(String.valueOf(jr.get("code"))) == 564) {
				log.warn("space exists!");
				return true;
			} else {
				throw new EFException("create Space "+this.dbName+" Exception," + jr.get("msg"));
			}
		} catch (Exception e) {
			log.error("create Space {} Exception",this.dbName, e);
			throw new EFException(e);
		}
	}

	public void writeSingle(String table, JSONObject datas) throws Exception {
		String response = EFHttpClientUtil.process(this.method + this.ROOTER + "/" + this.dbName + "/" + table,
				datas.toString());
		JSONObject jr = JSONObject.parseObject(response);
		if (Integer.valueOf(String.valueOf(jr.get("status"))) == 200)
			return;
		else {
			if(GlobalParam.DEBUG)
				log.warn(datas.toString());
			throw new EFException("Vearch error writing data," + jr.get("error"));
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
		}
	}
	

	public JSONObject search(String table,String query) throws Exception { 
		String response = EFHttpClientUtil.process(this.method + this.ROOTER + "/" + this.dbName + "/" + table + "/_search",
				HttpPost.METHOD_NAME,
				EFHttpClientUtil.DEFAULT_CONTENT_TYPE,
				query); 		
		return JSONObject.parseObject(response);	 
	}

	public void writeBatch(String table, CopyOnWriteArrayList<Object> datas) throws Exception {
		HttpPost rooter_post = new HttpPost(this.method + this.ROOTER + "/" + this.dbName + "/" + table + "/_bulk");
		rooter_post.addHeader("Content-Type", "application/json;charset=UTF-8");
		StringBuffer dt = new StringBuffer();
		int i = 0;
		if(datas.size()%2!=0) {
			throw new EFException("Dirty data Exception.");
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
			if (Integer.valueOf(String.valueOf(jo.get("status"))) != 200) {
				if(GlobalParam.DEBUG)
					log.warn(dt.toString());
				throw new EFException("Vearch error writing data," + jo.get("error"));
			}				
		}
	}

	public boolean close() {
		return true;
	}

	private void createDbifNotExists() {
		try {
			String response = EFHttpClientUtil.process(this.method + this.MASTER + listDb, HttpGet.METHOD_NAME,
					EFHttpClientUtil.DEFAULT_CONTENT_TYPE);
			JSONObject jr = JSONObject.parseObject(response);
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
		} catch (IOException e) {
			log.error("create Space Exception", e);
			return;
		}
	}
}
