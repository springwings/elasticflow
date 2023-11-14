/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.service;

import java.util.HashMap;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.handler.ResourceHandler;
import org.mortbay.resource.Resource;


/**
 *  * Monitor node all instances running
 * @author chengwen
 * @version 4.0
 * @date 2018-10-26 09:13
 */
public class EFWebService {  
	
	public void start() {
		HashMap<String, Object> serviceParams = new HashMap<String, Object>();
		serviceParams.put("confident_port", "8602");
		serviceParams.put("max_idle_time", "30000");
		serviceParams.put("port", "8616");
		serviceParams.put("thread_pool", "2");  
		try {
			ContextHandler _CH = new ContextHandler("/");
			ResourceHandler rh = new ResourceHandler();
			try { 
				rh.setBaseResource(Resource.newResource(GlobalParam.CONFIG_ROOT+"/efhead"));
			} catch (Exception e) {
				throw new EFException(e);
			}
			_CH.setHandler(rh); 
			serviceParams.put("httpHandle",_CH);  
			HttpService.getInstance(serviceParams).start();
			Common.LOG.info("Management Backend, http://{}:8616",GlobalParam.IP);
		} catch (EFException e) {
			Common.stopSystem(false);
		}
	} 
}
