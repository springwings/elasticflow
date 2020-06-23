package org.elasticflow.model.reader;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * store Level-2-seqs relative information like table level
 * @author chengwen
 * @version 1.0
 * @date 2018-11-26 13:43
 */
public class ScanPosition {

	private String instance;
	private String storeId;
	private ConcurrentHashMap<String, String> L2seqPos = new ConcurrentHashMap<>();
	private String JOB_STATE_SPERATOR = ":";
	private String JOB_SEQ_SPERATOR = ",";
	private String INFO_SPERATOR = "#";
	private String keep="";
	
	public ScanPosition(String info,String instance,String storeId) {
		String[] tmp = info.split(INFO_SPERATOR);
		if(tmp.length!=3) {
			this.instance = instance;
			this.storeId = storeId;
		}else {
			this.instance = tmp[0];
			this.storeId = tmp[1];
			String[] L2seqs = tmp[2].split(JOB_SEQ_SPERATOR);
			String[] row;
			for(String seq:L2seqs) {
				row = seq.split(JOB_STATE_SPERATOR);
				L2seqPos.put(row[0], row[1]);
			}
		}
	}
	
	public ScanPosition(String instance,String storeId) {
		this.instance = instance;
		this.storeId = storeId;
	}
	
	public void updateL2SeqPos(String k,String v) {
		L2seqPos.put(k, v);
	} 
	
	/**
	 * Store temporary full scan start location 
	 */
	public void keepCurrentPos() {
		keep = getPositionString();
	}
	
	public void recoverKeep() { 
		L2seqPos.clear();
		if(keep.length()>0) {
			String[] seqs = keep.split(JOB_SEQ_SPERATOR);
			String[] row;
			for(String seq:seqs) {
				row = seq.split(JOB_STATE_SPERATOR);
				L2seqPos.put(row[0], row[1]);
			}
		} 
	}
	
	public void batchUpdateSeqPos(String v) {
		Iterator<Map.Entry<String, String>> iter = L2seqPos.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, String> entry = (Entry<String, String>) iter.next();
			L2seqPos.put(entry.getKey(), v);
		}
	}
	
	public String getL2SeqPos(String seq) {
		if(L2seqPos.containsKey(seq)) {
			return L2seqPos.get(seq);
		}
		return "0";
	}
	
	public void updateStoreId(String storeId) {
		this.storeId = storeId;
	}
	
	
	public String getInstance() {
		return instance;
	}
 
	public String getStoreId() {
		return storeId==null?"":storeId;
	}
	
	public String getPositionString() {
		StringBuilder sf = new StringBuilder(); 
		Iterator<Map.Entry<String, String>> iter = L2seqPos.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, String> entry = (Entry<String, String>) iter.next();
			sf.append(entry.getKey()+JOB_STATE_SPERATOR+entry.getValue()+JOB_SEQ_SPERATOR);
		}
		return sf.toString();
	}
 
	public String getString() {
		StringBuilder sf = new StringBuilder();
		sf.append(instance);
		sf.append(INFO_SPERATOR);
		sf.append(storeId);
		sf.append(INFO_SPERATOR);
		sf.append(getPositionString());
		return sf.toString();
	}
}
