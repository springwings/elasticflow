package org.elasticflow.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.field.RiverField;
import org.elasticflow.model.reader.PipeDataUnit;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-02 13:53
 */
public final class SqlUtil {
	
	/**
	 * 
	 * @param tableSeq
	 * @param startKey
	 * @param endKey
	 * @param start_time
	 * @param end_time
	 * @param scanField
	 * @return
	 */
	public static HashMap<String, String> getScanParam(String L2seq,String startKey,String endKey,String start_time,String end_time,String scanField){
		HashMap<String, String> sqlParams = new HashMap<>();
		if (L2seq != null && L2seq.length() > 0)
			sqlParams.put(GlobalParam._seq, L2seq);
		sqlParams.put(GlobalParam._start, startKey);
		sqlParams.put(GlobalParam._end, endKey);
		sqlParams.put(GlobalParam._start_time, start_time);
		sqlParams.put(GlobalParam._end_time, end_time);
		sqlParams.put(GlobalParam._scan_field, scanField); 
		return sqlParams;
	}
	
	/**
	 * replace sql with params
	 * 
	 * @param sql
	 * @param seq
	 * @param startId
	 * @param maxId
	 * @param lastUpdateTime
	 * @param updateTime
	 * @return
	 */
	public static String fillParam(String originalSql, HashMap<String, String> params) {
		String res = originalSql;
		Iterator<String> entries = params.keySet().iterator();
		while (entries.hasNext()) {
			String k = entries.next();
			if (k.indexOf("#{") > -1)
				res = res.replace(k, params.get(k));
		}
		return res;
	} 
 
	public static String getWriteSql(String table,PipeDataUnit unit,Map<String, RiverField> transParams) {
		String sql = "INSERT INTO " + table;
		StringBuilder values = new StringBuilder();
		StringBuilder columns = new StringBuilder();
		for (Entry<String, Object> r : unit.getData().entrySet()) {
			String field = r.getKey();
			if (r.getValue() == null)
				continue;
			String value = String.valueOf(r.getValue());
			RiverField transParam = transParams.get(field);
			if (transParam == null)
				transParam = transParams.get(field.toLowerCase());
			if (transParam == null)
				continue;
			values.append("'" + value + "' ,");
			columns.append(transParam.getAlias() + " ,");
		}
		sql = sql + "(" + columns.substring(0, columns.length() - 1) + ") VALUES ("
				+ values.substring(0, values.length() - 1) + ")";
		return sql;
	}
}
