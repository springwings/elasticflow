/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.instruction.sets;

import java.util.List;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.instruction.Context;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.piper.PipePump;
import org.elasticflow.util.Common;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.yarn.Resource;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class TaskControl extends Instruction{ 
	
	public static void moveFullPosition(Context context, Object[] args) {
		if (!isValid(3, args)) {
			Common.LOG.error("moveFullPosition parameter not match!");
			return ;
		} 
		int start = Integer.parseInt(args[0].toString());
		int days = Integer.parseInt(args[1].toString());
		int ride = Integer.parseInt(args[2].toString());
		String[] L1seqs = Common.getL1seqs(context.getInstanceConfig(),true);  
		for(String L1seq:L1seqs) {
			String info = Common.getFullStartInfo(context.getInstanceConfig().getName(), L1seq);
			String saveInfo="";
			if(info!=null && info.length()>5) {
				for(String tm:info.split(",")) {
					if(Integer.parseInt(tm)<start) {
						saveInfo += String.valueOf(start+days*3600*24*ride)+",";
					}else {
						saveInfo += String.valueOf(Integer.parseInt(tm)+days*3600*24*ride)+",";
					} 
				}
			}else {
				saveInfo = String.valueOf(start + days*3600*24*ride);
			}
			EFDataStorer.setData(Common.getTaskStorePath(context.getInstanceConfig().getName(), L1seq,GlobalParam.JOB_FULLINFO_PATH),saveInfo);
		} 
	}
		
	public static void setIncrementPosition(Context context, Object[] args) {
		if (!isValid(1, args)) {
			Common.LOG.error("moveFullPosition parameter not match!");
			return ;
		} 		
		int position = Integer.parseInt(args[0].toString());
		String[] l1seqs = Common.getL1seqs(context.getInstanceConfig(),true);  
		for(String l1seq:l1seqs) {  
			List<String> L2Seq = context.getInstanceConfig().getReadParams().getL2Seq();
			PipePump transDataFlow = Resource.SOCKET_CENTER.getPipePump(context.getInstanceConfig().getName(), l1seq, false,GlobalParam.FLOW_TAG._DEFAULT.name());
			String storeId = GlobalParam.TASK_STATE.getStoreId(context.getInstanceConfig().getName(), l1seq, transDataFlow, true, false);
			if(storeId==null)
				break;
			for(String tseq:L2Seq) {
				GlobalParam.TASK_STATE.updateLSeqPos(context.getInstanceConfig().getName(), l1seq, tseq, String.valueOf(position));			
			}
			GlobalParam.TASK_STATE.saveTaskInfo(context.getInstanceConfig().getName(), l1seq, storeId,GlobalParam.JOB_INCREMENTINFO_PATH);
		}
	}
}
