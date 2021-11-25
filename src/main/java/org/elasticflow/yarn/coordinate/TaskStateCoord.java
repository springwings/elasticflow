package org.elasticflow.yarn.coordinate;

import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.model.Task;

public interface TaskStateCoord extends Coordination{
	
	public void setScanPosition(Task task,String scanStamp);
	
	public boolean checkFlowStatus(String instance,String seq,JOB_TYPE type,STATUS state);
	
	public boolean setFlowStatus(String instance,String L1seq,String type,STATUS needState, STATUS setState,boolean showLog);
	 
}
