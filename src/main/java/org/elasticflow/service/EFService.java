package org.elasticflow.service;

import java.util.HashMap;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:23
 */
public interface EFService {
	public void init(HashMap<String, Object> serviceParams);
	public void close();
	public void start();
}
