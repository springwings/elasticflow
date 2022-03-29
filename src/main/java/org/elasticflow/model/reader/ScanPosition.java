package org.elasticflow.model.reader;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONObject;

/**
 * store Level-2-seqs relative information like table level
 * @author chengwen
 * @version 1.0
 * @date 2018-11-26 13:43
 */
public class ScanPosition implements Serializable{

	private static final long serialVersionUID = -1408879403312612034L;
	private String instance;
	private String incrementStoreId;
	private String fullStoreId;
	/**L2seq Scan switch point location information which in Piper configuration file,Lseq is seq l1 + l2*/
	private ConcurrentHashMap<String, String> incrementLseqPos = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, String> fullLseqPos = new ConcurrentHashMap<>();
	private JSONObject keep = new JSONObject();
		
	public synchronized void loadInfos(String info,boolean isfull) {
		JSONObject datas = JSONObject.parseObject(info);
		if(datas.size()>2) {
			this.instance = datas.getString("instance");
			if(isfull) {
				this.fullStoreId = datas.getString("storeId");
				JSONObject infos = datas.getJSONObject("status");
				for (Entry<String, Object> entry : infos.entrySet()) {
					fullLseqPos.put(entry.getKey(), entry.getValue().toString());
				}
			}else {
				this.incrementStoreId = datas.getString("storeId");
				JSONObject infos = datas.getJSONObject("status");
				incrementLseqPos.clear();
				for (Entry<String, Object> entry : infos.entrySet()) {
					incrementLseqPos.put(entry.getKey(), entry.getValue().toString());
				}
			}			
		}
	}
	
	public ScanPosition(String instance,String incrementStoreId) {
		this.instance = instance;
		this.incrementStoreId = incrementStoreId;
	}
	
	/**
	 * 
	 * @param seq combine L1seq L2seq
	 * @param pos
	 */
	public synchronized void updateLSeqPos(String Lseq,String pos,boolean isfull) {		
		if(isfull) {
			fullLseqPos.put(Lseq, pos);
		}else {
			incrementLseqPos.put(Lseq, pos);
		}		
	} 
	
	/**
	 * Store temporary full scan start location 
	 */
	public void keepCurrentPos() {
		keep = getPositionDatas(false);
	}
	
	public void recoverKeep() { 
		incrementLseqPos.clear();
		if(keep.size()>0) {
			for (Entry<String, Object> entry : keep.entrySet()) {
				incrementLseqPos.put(entry.getKey(), entry.getValue().toString());
			}
			keep.clear();
		} 
	}
	
	public synchronized void batchUpdateSeqPos(String v,boolean isfull) {
		if(isfull) {
			Iterator<Map.Entry<String, String>> iter = fullLseqPos.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, String> entry = (Entry<String, String>) iter.next();
				fullLseqPos.put(entry.getKey(), v);
			}
		}else {
			Iterator<Map.Entry<String, String>> iter = incrementLseqPos.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, String> entry = (Entry<String, String>) iter.next();
				incrementLseqPos.put(entry.getKey(), v);
			}
		}		
	}
	
	public String getLSeqPos(String Lseq,boolean isfull) {
		if(isfull) {
			if(fullLseqPos.containsKey(Lseq)) 
				return fullLseqPos.get(Lseq);
		}else {
			if(incrementLseqPos.containsKey(Lseq)) 
				return incrementLseqPos.get(Lseq);
		}
		return "0";
	}
	
	public synchronized void updateStoreId(String storeId,boolean isfull) {
		if(isfull) {
			this.fullStoreId = storeId;
		}else {
			this.incrementStoreId = storeId;
		}		
	}
	
	
	public String getInstance() {
		return instance;
	}
 
	public String getStoreId(boolean isfull) {
		if(isfull) {
			return fullStoreId==null?"":fullStoreId;
		}
		return incrementStoreId==null?"":incrementStoreId;
	}
	
	public JSONObject getPositionDatas(boolean isfull) {
		JSONObject datas = new JSONObject();
		Iterator<Map.Entry<String, String>> iter;
		if(isfull) {
			iter = fullLseqPos.entrySet().iterator();
		}else {
			iter = incrementLseqPos.entrySet().iterator();
		}		
		while (iter.hasNext()) {
			Map.Entry<String, String> entry = (Entry<String, String>) iter.next();
			datas.put(entry.getKey(), entry.getValue());
		}
		return datas;
	}
	
	public String getString(boolean isfull) {
		JSONObject datas = new JSONObject();
		datas.put("instance", instance);
		datas.put("storeId", isfull?fullStoreId:incrementStoreId);
		datas.put("status", getPositionDatas(isfull));
		return datas.toJSONString();
	}
}
