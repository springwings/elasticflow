package org.elasticflow.instruction.sets;

import org.elasticflow.instruction.Context;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.util.Common;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class SplitData extends Instruction{ 
	
	public static double getSplitDayPoint(Context context, Object[] args) {
		double time = 0;
		if (!isValid(1, args)) {
			Common.LOG.error("getSplitDayPoint parameter not match!");
			return time;
		}
		int days = Integer.parseInt(String.valueOf(args[0]));
		time = System.currentTimeMillis()-days*3600*1000;
		return time; 
	}
}
