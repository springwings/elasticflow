package org.elasticflow.yarn.coordinator;

import org.elasticflow.util.Common;
import org.elasticflow.yarn.coord.NodeCoord;

/**
 * Cluster node operation control Coordinator
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */

public class NodeCoordinator implements NodeCoord{

	@Override
	public void stopNode() {
		Common.stopSystem();		
	}

	@Override
	public void restartNode() {
		// TODO Auto-generated method stub
		
	}

}
