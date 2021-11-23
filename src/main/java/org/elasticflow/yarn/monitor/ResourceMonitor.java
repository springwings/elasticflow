package org.elasticflow.yarn.monitor;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.correspond.ReportStatus;
import org.elasticflow.util.instance.EFDataStorer;

import com.alibaba.fastjson.JSON;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-12-04 13:39
 */

public class ResourceMonitor {  
	
	public static void start() {
		ReportStatus.nodeConfigs();
		EFDataStorer.setData(GlobalParam.CONFIG_PATH + "/EF_NODES/" + GlobalParam.NODEID + "/configs", JSON.toJSONString(GlobalParam.StartConfig)); 
		statusMonitor();
	}
	
	private static void statusMonitor() { 
	    new Thread(() -> { 
	        while (true) { 
	            try { 
	               
	                Thread.sleep(3000); 
	            } catch (Exception e) { 
	                e.printStackTrace(); 
	            } 
	        } 
	    }).start(); 
	} 
}
