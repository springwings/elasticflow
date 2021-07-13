package org.elasticflow.connect;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticflow.util.EFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.minidev.json.JSONValue;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class VearchConnector {

	final String MASTER_PORT = "8817";

	final String ROOTER_PORT = "9001";

	protected String path;

	protected String dbName;

	protected CloseableHttpClient httpClient = HttpClients.createDefault();
 
	protected JSONObject dbObject;

	protected String method = "http://";
	
	final String listDb = "/list/db";
	
	final String createDb = "/db/_create"; 
	
	private final static Logger log = LoggerFactory.getLogger(VearchConnector.class);
	
	public VearchConnector(String path, String dbName) {
		this.path = path;
		this.dbName = dbName; 
		this.dbObject = new JSONObject();
		this.dbObject.put("name", this.dbName);
	}

	public boolean deleteSpace(String space) {
		HttpDelete master_delete = new HttpDelete(this.method + this.path + ":" + this.MASTER_PORT+"/space/"+this.dbName+"/"+space);
		master_delete.addHeader("Content-Type", "application/json;charset=UTF-8");
        try {
        	CloseableHttpResponse response = this.httpClient.execute(master_delete);
			JSONObject jr = JSONObject.fromObject(this.getContent(response));
			if(Integer.valueOf(String.valueOf(jr.get("code")))==200) {
				return true;
			}else {
				log.error("delete Space Exception,"+jr.get("msg"));
			}
		} catch (Exception e) { 
			log.error("delete Space Exception",e);
		}
        return false;
	}

	public boolean createSpace(JSONObject tableMeta) throws EFException {
		this.createDbifNotExists();
		HttpPut master_post = new HttpPut(this.method + this.path + ":" + this.MASTER_PORT+"/space/"+this.dbName+"/_create");
		master_post.addHeader("Content-Type", "application/json;charset=UTF-8");
		StringEntity stringEntity = new StringEntity(tableMeta.toString(), "UTF-8");
        stringEntity.setContentEncoding("UTF-8");
        master_post.setEntity(stringEntity);
        try {
        	CloseableHttpResponse response = this.httpClient.execute(master_post);
			JSONObject jr = JSONObject.fromObject(this.getContent(response));
			if(Integer.valueOf(String.valueOf(jr.get("code")))==200)
				return true;
			else if(Integer.valueOf(String.valueOf(jr.get("code")))==564){
				log.warn("space exists!");
				return true;
			}else {
				throw new EFException("createSpace Exception,"+jr.get("msg"));
			}
		} catch (Exception e) { 
			log.error("createSpace Exception",e);
			throw new EFException(e);
		}
	}
	
	public void writeSingle(String table,JSONObject datas) throws Exception {
		HttpPost rooter_post = new HttpPost(this.method + this.path + ":" + this.ROOTER_PORT+"/"+this.dbName+"/"+table);
		rooter_post.addHeader("Content-Type", "application/json;charset=UTF-8");
		StringEntity stringEntity = new StringEntity(datas.toString(), "UTF-8");
        stringEntity.setContentEncoding("UTF-8");
        rooter_post.setEntity(stringEntity); 
        CloseableHttpResponse response = this.httpClient.execute(rooter_post);
        JSONObject jr = JSONObject.fromObject(this.getContent(response));
		if(Integer.valueOf(String.valueOf(jr.get("status")))==200)
			return;
		else {
			throw new EFException("write data Exception,"+jr.get("error"));
		}
	}
	
	public void writeBatch(String table,CopyOnWriteArrayList<Object> datas) throws Exception {
		HttpPost rooter_post = new HttpPost(this.method + this.path + ":" + this.ROOTER_PORT+"/"+this.dbName+"/"+table+"/_bulk");
		rooter_post.addHeader("Content-Type", "application/json;charset=UTF-8");
		StringBuffer sb = new StringBuffer(); 
		int i = 0;
		while(i<datas.size()) {
			sb.append("\n"+String.valueOf(datas.get(i))+"\n");
			sb.append(datas.get(i+1).toString());
			i+=2;
		}
		StringEntity params = new StringEntity(sb.toString().trim());
		params.setContentEncoding("UTF-8");
        rooter_post.setEntity(params); 
        HttpResponse response = this.httpClient.execute(rooter_post);
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent())); 
        Object jr =  JSONValue.parse(rd); 
        JSONArray ja = JSONArray.fromObject(jr);
        for(int j=0;j<ja.size();j++) {
        	JSONObject jo = JSONObject.fromObject(ja.get(j));
        	if(Integer.valueOf(String.valueOf(jo.get("status")))!=200)
        		throw new EFException("write data Exception,"+jo.get("error"));
        }
	}

	public boolean close() {
		return true;
	}

	private void createDbifNotExists(){
		CloseableHttpResponse response;
		try {
			response = this.httpClient
					.execute(new HttpGet(this.method + this.path + ":" + this.MASTER_PORT + listDb));
			JSONObject jr = JSONObject.fromObject(this.getContent(response));
			JSONArray jsonArr = JSONArray.fromObject(jr.get("data"));
			@SuppressWarnings("unchecked")
			Iterator<Object> it = jsonArr.iterator();
			while (it.hasNext()) {
				JSONObject jsonObj = (JSONObject) it.next();
				if (jsonObj.get("name").equals(this.dbName)) {
					return;
				}
			}
			HttpPut master_post = new HttpPut(this.method + this.path + ":" + this.MASTER_PORT+createDb);
			master_post.addHeader("Content-Type", "application/json;charset=UTF-8");
			StringEntity stringEntity = new StringEntity(this.dbObject.toString(), "UTF-8");
	        stringEntity.setContentEncoding("UTF-8");
	        master_post.setEntity(stringEntity);
	        this.httpClient.execute(master_post);
		} catch (IOException e) { 
			log.error("createSpace Exception",e);
			return;
		}
	}
	
	private String getContent(CloseableHttpResponse response) throws IOException {
		StringBuffer result = new StringBuffer();
		try (BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
			String line;
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
		}
		return result.toString();
	} 
	public static void main(String[] args) {
		VearchConnector vConnector = new VearchConnector("192.168.6.156", "vehicle");
		vConnector.deleteSpace("vehicle_vec_1626019200");
	}
}
