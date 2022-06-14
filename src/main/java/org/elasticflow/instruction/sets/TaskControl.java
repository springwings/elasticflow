/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.instruction.sets;

import java.util.List;
import java.util.Map.Entry;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.instruction.Context;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.piper.PipePump;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.yarn.Resource;

import com.alibaba.fastjson.JSONObject;

/**
 * Task Control Instruction sets
 * It is an instruction code,Can only interpret calls
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class TaskControl extends Instruction {

	public static void moveFullPosition(Context context, Object[] args) {
		if (!isValid(3, args)) {
			Common.LOG.error("moveFullPosition parameter not match!");
			return;
		}
		int start = Integer.parseInt(args[0].toString());
		int days = Integer.parseInt(args[1].toString());
		int ride = Integer.parseInt(args[2].toString());
		JSONObject infos = GlobalParam.TASK_COORDER.getInstanceScanDatas(context.getInstanceConfig().getInstanceID(), true);
		JSONObject saveInfo = new JSONObject();
		for (Entry<String, Object> entry : infos.entrySet()) {
			if (Integer.parseInt(entry.getValue().toString()) < start) {
				saveInfo.put(entry.getKey(), start + days * 3600 * 24 * ride);
			} else {
				saveInfo.put(entry.getKey(), Integer.parseInt(entry.getValue().toString()) + days * 3600 * 24 * ride);
			}
		}
		EFDataStorer.setData(
				Common.getTaskStorePath(context.getInstanceConfig().getInstanceID(), GlobalParam.JOB_FULLINFO_PATH),
				saveInfo.toJSONString());
	}

	public static void setIncrementPosition(Context context, Object[] args) throws EFException {
		if (!isValid(1, args)) {
			Common.LOG.error("move full position parameter not match!");
			return;
		}
		int position = Integer.parseInt(args[0].toString());
		String[] l1seqs = Common.getL1seqs(context.getInstanceConfig());
		for (String l1seq : l1seqs) {
			List<String> L2Seq = context.getInstanceConfig().getReadParams().getL2Seq();
			PipePump pipePump = Resource.socketCenter.getPipePump(context.getInstanceConfig().getInstanceID(), l1seq,
					false, GlobalParam.FLOW_TAG._DEFAULT.name());
			String storeId = GlobalParam.TASK_COORDER.getStoreId(context.getInstanceConfig().getInstanceID(), l1seq,
					pipePump.getID(), true, false);
			if (storeId == null)
				break;
			for (String tseq : L2Seq) {
				GlobalParam.TASK_COORDER.setScanPositon(context.getInstanceConfig().getInstanceID(), l1seq, tseq,
						String.valueOf(position), false);
			}
			GlobalParam.TASK_COORDER.saveTaskInfo(context.getInstanceConfig().getInstanceID(), l1seq, storeId, false);
		}
	}
}
