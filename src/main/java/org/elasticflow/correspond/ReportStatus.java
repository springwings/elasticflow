package org.elasticflow.correspond;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.ConfigStorer;
 
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
			if (ConfigStorer.exists(GlobalParam.CONFIG_PATH)==false) {
				String path = "";
				for (String str : GlobalParam.CONFIG_PATH.split("/")) {
					path += "/" + str;
					ConfigStorer.createPath(path,false);
				} 
			}
			if (ConfigStorer.exists(GlobalParam.CONFIG_PATH+"/INSTANCES")==false)
				ConfigStorer.createPath(GlobalParam.CONFIG_PATH+"/INSTANCES",false);
			if (ConfigStorer.exists(GlobalParam.CONFIG_PATH+"/instructions.xml")==false)
				ConfigStorer.createPath(GlobalParam.CONFIG_PATH+"/instructions.xml",true);
			if (ConfigStorer.exists(GlobalParam.CONFIG_PATH+"/resource.xml")==false) 
				ConfigStorer.createPath(GlobalParam.CONFIG_PATH+"/resource.xml",true);
			
			if (ConfigStorer.exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES")==false) {
				ConfigStorer.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES",false);
			}
			if (ConfigStorer.exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP)==false) {
				ConfigStorer.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP,false);
			}
			if (ConfigStorer.exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP + "/configs")==false) {
				ConfigStorer.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP + "/configs",true);
			}
		} catch (Exception e) {
			Common.LOG.error("environmentCheck Exception", e);
		}
	}
	
}
