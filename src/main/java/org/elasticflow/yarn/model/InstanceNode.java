package org.elasticflow.yarn.model;

import org.elasticflow.node.Node;

/**
 * @description
 * @author chengwen
 * @version 1.0
 * @date 2018-11-13 10:53
 */

public class InstanceNode {

	String instace;
	Node node;

	public String getInstace() {
		return instace;
	}

	public void setInstace(String instace) {
		this.instace = instace;
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

}
