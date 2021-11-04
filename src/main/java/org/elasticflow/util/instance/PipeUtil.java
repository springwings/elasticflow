/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.util.instance;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;


/**
 * 
 * @author chengwen
 * @version 4.x
 * @date 2018-11-02 13:53
 * @modify 2019-01-16 13:54
 */
public final class PipeUtil {
	
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
		HashMap<String, String> params = new HashMap<>();
		if (L2seq != null && L2seq.length() > 0)
			params.put(GlobalParam._seq, L2seq);
		params.put(GlobalParam._start, startKey);
		params.put(GlobalParam._end, endKey);
		params.put(GlobalParam._start_time, start_time);
		params.put(GlobalParam._end_time, end_time);
		params.put(GlobalParam._scan_field, scanField); 
		return params;
	}
	
	/**
	 * replace sql with params
	 * 
	 * @param scanDSL
	 * @param seq
	 * @param startId
	 * @param maxId
	 * @param lastUpdateTime
	 * @param updateTime
	 * @return
	 */
	public static String fillParam(String scanDSL, HashMap<String, String> params) {
		if(scanDSL!=null) {
			Iterator<String> entries = params.keySet().iterator();
			while (entries.hasNext()) {
				String k = entries.next();
				if (k.indexOf("#{") > -1)
					scanDSL = scanDSL.replace(k, params.get(k));
			}
			return scanDSL;
		} 
		return null;
	} 
 
	public static String getWriteSql(String table,PipeDataUnit unit,Map<String, EFField> transParams) { 		
		return getWriteSqlHead(table, unit, transParams) + getWriteSqlTailData(table, unit, transParams);
	}
	
	public static String getWriteSqlTailData(String table,PipeDataUnit unit,Map<String, EFField> transParams) {		
		StringBuilder values = new StringBuilder();
		for (Entry<String, Object> r : unit.getData().entrySet()) {
			String field = r.getKey();
			if (r.getValue() == null)
				continue;
			String value = String.valueOf(r.getValue());
			EFField transParam = transParams.get(field);
			if (transParam == null)
				transParam = transParams.get(field.toLowerCase());
			if (transParam == null)
				continue;
			values.append("'" + value + "' ,");
		}
		
		return "("+values.substring(0, values.length() - 1)+ ")";
	}
	
	public static String getWriteSqlHead(String table,PipeDataUnit unit,Map<String, EFField> transParams) {
		String sql = "INSERT INTO " + table;
		StringBuilder columns = new StringBuilder();
		for (Entry<String, Object> r : unit.getData().entrySet()) {
			String field = r.getKey();
			if (r.getValue() == null)
				continue;
			EFField transParam = transParams.get(field);
			if (transParam == null)
				transParam = transParams.get(field.toLowerCase());
			if (transParam == null)
				continue;
			columns.append(transParam.getAlias() + " ,");
		}
		sql = sql + "(" + columns.substring(0, columns.length() - 1) + ") VALUES ";
		return sql;
	}
	

	public static boolean scanPosCompare(String s1, String s2) {
		if (s1.compareTo(s2) > 0 || s1.length() > s2.length())
			return true;
		return false;
	}
	
	public static int estimateThreads(int taskPageNum) {
		int num = (int) (taskPageNum/3);
		if(num<2) {
			return 1;
		}else if (num>20) {
			return 20;
		}else {
			return num;
		}
	}
}
