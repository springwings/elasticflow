package org.elasticflow.yarn.coord;

/**
 * Cluster node operation control
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */

public interface NodeCoord extends Coordination{

	public void stopNode();
	
	public void restartNode();
	
}
