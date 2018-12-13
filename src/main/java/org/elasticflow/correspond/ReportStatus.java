package org.elasticflow.correspond;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.ZKUtil;
 
/**
 * report node machine status with heartBeat and correspond job status
 * @author chengwen
 * @version 2.0
 * @date 2018-11-21 15:43
 */
public final class ReportStatus {  
	 
	public static void jobState() {
		 
	}
	
	public static void nodeConfigs(){
		try {
			if (ZKUtil.getZk().exists(GlobalParam.CONFIG_PATH, true) == null) {
				String path = "";
				for (String str : GlobalParam.CONFIG_PATH.split("/")) {
					path += "/" + str;
					ZKUtil.createPath(path, true);
				}
				ZKUtil.createPath(path+"/INSTANCES", true);
				
				if (ZKUtil.getZk().exists(path+"/instructions.xml", true) == null) 
					ZKUtil.createPath(path+"/instructions.xml", true);
				if (ZKUtil.getZk().exists(path+"/resource.xml", true) == null) 
					ZKUtil.createPath(path+"/resource.xml", true);
			}
			if (ZKUtil.getZk().exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES", false) == null) {
				ZKUtil.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES", true);
			}
			if (ZKUtil.getZk().exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP, false) == null) {
				ZKUtil.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP, true);
			}
			if (ZKUtil.getZk().exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP + "/configs",
					false) == null) {
				ZKUtil.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP + "/configs", true);
			}
		} catch (Exception e) {
			Common.LOG.error("environmentCheck Exception", e);
		}
	}
}
