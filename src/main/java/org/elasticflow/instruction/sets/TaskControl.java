package org.elasticflow.instruction.sets;

import java.util.List;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.instruction.Context;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.piper.PipePump;
import org.elasticflow.util.Common;
import org.elasticflow.util.ConfigStorer;
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
			ConfigStorer.setData(Common.getTaskStorePath(context.getInstanceConfig().getName(), L1seq,GlobalParam.JOB_FULLINFO_PATH),saveInfo);
		} 
	}
	
	public static void setIncrementPosition(Context context, Object[] args) {
		if (!isValid(1, args)) {
			Common.LOG.error("moveFullPosition parameter not match!");
			return ;
		} 
		
		int position = Integer.parseInt(args[0].toString());
		String[] seqs = Common.getL1seqs(context.getInstanceConfig(),true);  
		String instanceName;
		for(String seq:seqs) {  
			instanceName = Common.getMainName(context.getInstanceConfig().getName(), seq);
			List<String> table_seq = context.getInstanceConfig().getReadParams().getSeq();
			PipePump transDataFlow = Resource.SOCKET_CENTER.getPipePump(context.getInstanceConfig().getName(), seq, false,GlobalParam.FLOW_TAG._DEFAULT.name());
			String storeId = Common.getStoreId(context.getInstanceConfig().getName(), seq, transDataFlow, true, false);
			if(storeId==null)
				break;
			for(String tseq:table_seq) {
				GlobalParam.SCAN_POSITION.get(instanceName).updateL2SeqPos(tseq, String.valueOf(position));  
			}
			Common.saveTaskInfo(context.getInstanceConfig().getName(), seq, storeId,GlobalParam.JOB_INCREMENTINFO_PATH);
		}
	}
}
