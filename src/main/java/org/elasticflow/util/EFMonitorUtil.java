package org.elasticflow.util;

import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.node.NodeMonitor;
import org.mortbay.jetty.Request;

/**
 * @author chengwen
 * @version 3.0
 * @date 2018-10-25 09:08
 */

public class EFMonitorUtil {
	
	public static boolean checkParams(NodeMonitor obj,Request rq,String checkParams){ 
		for(String param:checkParams.split(",")) {
			if(rq.getParameter(param)==null || rq.getParameter(param).strip().equals("")) {
				obj.setResponse(RESPONSE_STATUS.ParameterErr, checkParams+" parameters may be missing!", null);
				return false;
			}
		} 
		return true;
	}	
}
