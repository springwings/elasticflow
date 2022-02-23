/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.service;

import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.EFSearchRequest;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.yarn.Resource;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;


/**
 *  * Monitor node all instances running
 * @author chengwen
 * @version 4.0
 * @date 2018-10-26 09:13
 */
public class EFMonitorService {  
	
	public void start() {
		HashMap<String, Object> serviceParams = new HashMap<String, Object>();
		serviceParams.put("confident_port", "8601");
		serviceParams.put("max_idle_time", "30000");
		serviceParams.put("port", "8617");
		serviceParams.put("thread_pool", "3");
		serviceParams.put("httpHandle", new httpHandle());
		try {
			HttpService.getInstance(serviceParams).start();
		} catch (EFException e) {
			Common.stopSystem();
		}
	}
	
	public class httpHandle extends AbstractHandler {
		@Override
		public void handle(String target, HttpServletRequest request,
				HttpServletResponse response, int dispatch) throws IOException,
				ServletException {
			response.setContentType("application/json;charset=utf8");
			response.setStatus(HttpServletResponse.SC_OK);

			Request rq = (request instanceof Request) ? (Request) request
					: HttpConnection.getCurrentConnection().getRequest(); 
			String dataTo = rq.getPathInfo().substring(1);
			EFSearchRequest RR = Common.getRequest(rq);
			EFResponse rps = EFResponse.getInstance();
			rps.setRequest(RR.getParams());
			switch (dataTo) {  
			case "efm.doaction":{
				Resource.nodeMonitor.ac(rq,rps);				
			}
				break;   
			case "_version": 
				rps.setInfo(GlobalParam.VERSION);  
				break;
			default:
				HashMap<String, Object> info = new HashMap<>();
				info.put("Method", new String[] {"/efm.doaction","/_version"});
				info.put("Example", " /efm.doaction?ac=getInstanceInfo&instance=demo");
				rps.setPayload(info);
				break;
			}
			response.getWriter().println(rps.getResponse(true));
			response.getWriter().flush();
			response.getWriter().close();
		}
	}  
}
