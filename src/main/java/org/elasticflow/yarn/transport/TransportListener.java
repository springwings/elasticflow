package org.elasticflow.yarn.transport;


/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2021-11-12 13:39
 */
public interface TransportListener {
	
	default void onRequestReceived(long requestId, String action) {}
	
}
