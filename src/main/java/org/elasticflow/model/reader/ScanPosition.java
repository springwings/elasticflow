package org.elasticflow.model.reader;

import java.io.Serializable;
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
public class ScanPosition implements Serializable{

	private static final long serialVersionUID = -1408879403312612034L;
	private String instance;
	private String storeId;
	/**L2seq Scan switch point location information which in Piper configuration file,Lseq is seq l1 + l2*/
	private ConcurrentHashMap<String, String> LseqPos = new ConcurrentHashMap<>();
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
				LseqPos.put(row[0], row[1]);
			}
		}
	}
	
	public ScanPosition(String instance,String storeId) {
		this.instance = instance;
		this.storeId = storeId;
	}
	
	/**
	 * 
	 * @param seq combine L1seq L2seq
	 * @param pos
	 */
	public void updateLSeqPos(String seq,String pos) {		
		LseqPos.put(seq, pos);
	} 
	
	/**
	 * Store temporary full scan start location 
	 */
	public void keepCurrentPos() {
		keep = getPositionString();
	}
	
	public void recoverKeep() { 
		LseqPos.clear();
		if(keep.length()>0) {
			String[] seqs = keep.split(JOB_SEQ_SPERATOR);
			String[] row;
			for(String seq:seqs) {
				row = seq.split(JOB_STATE_SPERATOR);
				LseqPos.put(row[0], row[1]);
			}
		} 
	}
	
	public void batchUpdateSeqPos(String v) {
		Iterator<Map.Entry<String, String>> iter = LseqPos.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, String> entry = (Entry<String, String>) iter.next();
			LseqPos.put(entry.getKey(), v);
		}
	}
	
	public String getLSeqPos(String seq) {
		if(LseqPos.containsKey(seq)) {
			return LseqPos.get(seq);
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
		Iterator<Map.Entry<String, String>> iter = LseqPos.entrySet().iterator();
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
