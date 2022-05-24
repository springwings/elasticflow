package org.elasticflow.model;

import java.text.MessageFormat;
import java.util.HashMap;

import org.elasticflow.config.GlobalParam;

/**
 * Localization
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class Localization {

	public static enum LAG_TYPE {
		flowDisconnect,flowBreaker
	}
	
	final static HashMap<String, String> lOC_MAP = new HashMap<String, String>() {
		private static final long serialVersionUID = -8313429841889556616L;
		{
			put("flowDisconnect_ZH", "实例数据流已断开连接!");
			put("flowDisconnect_EN", "Instance data flow has been disconnected!");
			put("flowBreaker_ZH", "{0} 实例断路器打开!");
			put("flowBreaker_EN", "instance {0} breaker is on!");
		}
	};
	
	public static String format(LAG_TYPE lag) {
		if(GlobalParam.LANG=="ZH") {
			return lOC_MAP.get(lag.name()+"_ZH");
		}else {
			return lOC_MAP.get(lag.name()+"_EN");
		}		
	}	
	
	public static String format(LAG_TYPE lag,Object...params) {
		if(GlobalParam.LANG=="ZH") {
			return MessageFormat.format(lOC_MAP.get(lag.name()+"_ZH"), params);
		}else {
			return MessageFormat.format(lOC_MAP.get(lag.name()+"_ZH"), params);
		}		
	}	 
}
