/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.util.instance;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.yarn.Resource;


/**
 * Task Utils Package
 * 
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */
public class TaskUtil {
	 
	
	/**
	 * get Store Task Info
	 * if file not exists will create
	 * @param instance
	 * @param isfull
	 * @return
	 */
	public static String getStoreTaskInfo(String instance,boolean isfull) {
		String info = "{}";
		String path = TaskUtil.getInstanceStorePath(instance, 
				isfull?GlobalParam.JOB_FULLINFO_PATH:GlobalParam.JOB_INCREMENTINFO_PATH);
		byte[] b = EFDataStorer.getData(path, true);
		if (b != null && b.length > 0) {
			String str = new String(b);
			if (str.length() > 1) {
				info = str;
			}
		}
		return info;
	}

	/**
	 * get instance ProcessId
	 * instance running process ID
	 * @param L1seq    for data source sequence tag
	 * @param instance data source main tag name
	 * @return String
	 */
	public static String getInstanceProcessId(String instance, String L1seq) {
		if (L1seq != null && L1seq.length() > 0) {
			return instance + L1seq;
		} else {
			return instance;
		}
	}
 
	/**
	 * get instance data store path
	 * @param instanceName
	 * @param location
	 * @return
	 */
	public static String getInstanceStorePath(String instanceName, String location) {
		return GlobalParam.INSTANCE_PATH + "/" + instanceName + "/" + location;
	}
	
	/**
	 * 
	 * @param instance
	 * @param L1seq
	 * @param tag
	 * @param ignoreSeq    Is L1 added to form identification
	 * @return
	 */
	public static String getResourceTag(String instance, String L1seq, String tag, boolean ignoreSeq) {
		StringBuilder tags = new StringBuilder();
		if (!ignoreSeq && L1seq != null && L1seq.length() > 0) {
			tags.append(instance).append(L1seq); //L1seq Differentiate the use of resources
		} else {
			tags.append(instance).append(GlobalParam.DEFAULT_RESOURCE_SEQ);
		}
		return tags.append(tag).toString();
	}

	/**
	 * get read data source seq flags
	 * 
	 * @param instanceName
	 * @param fillDefault  if empty fill with system default blank seq
	 * @return
	 * @throws EFException 
	 */
	public static String[] getL1seqs(InstanceConfig instanceConfig) throws EFException {
		String[] seqs = {};
		WarehouseParam wp = Resource.nodeConfig.getWarehouse().get(instanceConfig.getPipeParams().getReadFrom());
		if (null != wp) {
			seqs = wp.getL1seq();
		} else {
			throw new EFException(instanceConfig.getPipeParams().getReadFrom()+" socket not exist in the file resource.xml.", ELEVEL.Termination);
		}
		return seqs;
	}
	
	
	/**
	 * Cluster homogeneous information
	 * @param messageId  field value for routing calculation
	 * @param routes  Number of scattered routes required
	 * @return
	 */
	public static int routeMessage(String messageId,int routes) { 
        int hashCode = messageId.hashCode(); 
        return Math.abs(hashCode) % routes;
	} 
	
	public static String getLseq(String L1seq, String L2seq) {
		if(L1seq=="" && L2seq=="")
			return "_";
		if(L1seq!="") {
			if(L2seq!="")
				return L1seq + "." + L2seq;
			return L1seq;
		}
		return L2seq;		
	}
	
	/**
	 * @param instanceName data source main tag name
	 * @param storeId      a/b or time mechanism tags
	 * @return String
	 */
	public static String getStoreName(String instanceName, String storeId) {
		if (storeId != null && storeId.length() > 0) {
			return instanceName + "_" + storeId;
		} else {
			return instanceName;
		}

	}
	
	/**
	 * Format output task execution log
	 * @param types
	 * @param heads    
	 * @param instanceName
	 * @param storeId
	 * @param L2seq    table seq
	 * @param total
	 * @param dataBoundary
	 * @param lastUpdateTime
	 * @param useTime
	 * @param moreinfo
	 * @return
	 */
	public static String formatLog(String types, String heads, String instanceName, String storeId, String L2seq,
			int total, String dataBoundary, String lastUpdateTime, long useTime, String moreinfo) {
		String useTimeFormat = Common.seconds2time(useTime);
		if (L2seq.length() < 1)
			L2seq = "None";
		String update;
		if (lastUpdateTime.length() > 9 && lastUpdateTime.matches("[0-9]+")) {
			update = Common.FormatTime(
					lastUpdateTime.length() < 12 ? Long.valueOf(lastUpdateTime + "000") : Long.valueOf(lastUpdateTime));
		} else {
			update = lastUpdateTime;
		}
		StringBuilder sb = new StringBuilder();
		switch (types) {
		case "complete":
			sb.append("[Complete " + heads + " " + instanceName + "_" + storeId + "] " + (" L2seq:" + L2seq));
			sb.append(" Docs:" + total);
			sb.append(" scanAt:" + update);
			sb.append(" useTime:" + useTimeFormat);
			break;
		case "start":
			sb.append("[Start " + heads + " " + instanceName + "_" + storeId + "] " + (" L2seq:" + L2seq));
			sb.append(" scanAt:" + update);
			break;
		default:
			sb.append("[ -- " + heads + " " + instanceName + "_" + storeId + "] " + (" L2seq:" + L2seq));
			sb.append(
					" Docs:" + total + (total == 0 || dataBoundary.length() < 1 ? "" : " dataBoundary:" + dataBoundary)
							+ " scanAt:" + update + " useTime:" + useTimeFormat);
			break;
		}
		return sb.append(moreinfo).toString();
	}
	
}
