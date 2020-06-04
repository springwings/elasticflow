package org.elasticflow.correspond;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.NodeStorer;
 
/**
 * report node machine status with heartBeat and correspond job status
 * @author chengwen
 * @version 2.0
 * @date 2018-11-21 15:43
 */
public final class ReportStatus {  
	 
	public static void jobState() {
		 
	}
	
	public static void nodeConfigs(boolean useZk){
		try {
			NodeStorer.setUseType(useZk);
			if (NodeStorer.exists(GlobalParam.CONFIG_PATH)==false) {
				String path = "";
				for (String str : GlobalParam.CONFIG_PATH.split("/")) {
					path += "/" + str;
					NodeStorer.createPath(path,false);
				} 
			}
			if (NodeStorer.exists(GlobalParam.CONFIG_PATH+"/INSTANCES")==false)
				NodeStorer.createPath(GlobalParam.CONFIG_PATH+"/INSTANCES",false);
			if (NodeStorer.exists(GlobalParam.CONFIG_PATH+"/instructions.xml")==false)
				NodeStorer.createPath(GlobalParam.CONFIG_PATH+"/instructions.xml",true);
			if (NodeStorer.exists(GlobalParam.CONFIG_PATH+"/resource.xml")==false) 
				NodeStorer.createPath(GlobalParam.CONFIG_PATH+"/resource.xml",true);
			
			if (NodeStorer.exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES")==false) {
				NodeStorer.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES",false);
			}
			if (NodeStorer.exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP)==false) {
				NodeStorer.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP,false);
			}
			if (NodeStorer.exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP + "/configs")==false) {
				NodeStorer.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP + "/configs",true);
			}
		} catch (Exception e) {
			Common.LOG.error("environmentCheck Exception", e);
		}
	}
	
}
