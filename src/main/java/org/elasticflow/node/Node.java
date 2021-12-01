package org.elasticflow.node;


/**
 * Node Model
 * 
 * @author chengwen
 * @version 1.0
 */
public class Node {

	private String ip;
	private boolean status;
	private String nodeId;
	
	public static Node getInstance(String ip,String nodeId) {
		Node n = new Node(ip,nodeId);
		return n;
	}
	
	public Node(String ip,String nodeId) {
		setIp(ip);
		setNodeId(nodeId);
	}
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

}
