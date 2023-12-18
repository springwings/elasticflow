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
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.EFRequest;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.yarn.Resource;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

/**
 * ElasticFlow searcher, support rest service
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class SearcherService{
 
	private EFService FS;
	 
	public boolean start() {
		HashMap<String, Object> serviceParams = new HashMap<String, Object>();
		serviceParams.put("confident_port", GlobalParam.SystemConfig.get("searcher_service_confident_port"));
		serviceParams.put("max_idle_time", GlobalParam.SystemConfig.get("searcher_service_max_idle_time"));
		serviceParams.put("port", GlobalParam.SystemConfig.get("searcher_service_port"));
		serviceParams.put("thread_pool", GlobalParam.SystemConfig.get("searcher_service_thread_pool"));
		serviceParams.put("httpHandle", new httpHandle());
		FS=HttpService.getInstance(serviceParams);		
		try {
			FS.start();
			Common.LOG.info("Searcher Service, http://{}:{}",GlobalParam.IP,GlobalParam.SystemConfig.get("searcher_service_port"));
		} catch (EFException e) {
			Common.stopSystem(false);
		}
		return true;
	}
	
	public boolean close(){
		if(FS!=null){
			FS.close();
		} 
		return true;
	} 
	
	public EFResponse process(EFRequest request,EFResponse response) throws Exception { 
		response.setStartTime(System.currentTimeMillis());
		String pipe = request.getPipe(); 
		Map<String, InstanceConfig> configMap = Resource.nodeConfig.getSearchConfigs();
		if (configMap.containsKey(pipe))  
			Resource.socketCenter.getSearcher(pipe,"","",false).startSearch(request,response); 
		response.setEndTime(System.currentTimeMillis());  
		return response;
	}

	class httpHandle extends AbstractHandler {
		@Override
		public void handle(String target, HttpServletRequest request,
				HttpServletResponse response, int dispatch) throws IOException,
				ServletException {
			Request rq = (request instanceof Request) ? (Request) request
					: HttpConnection.getCurrentConnection().getRequest();
			
			response.setContentType("application/json;charset=utf8");
			response.setStatus(HttpServletResponse.SC_OK);
			response.setHeader("PowerBy", GlobalParam.PROJ); 
			rq.setHandled(true);
			EFResponse rps = EFResponse.getInstance(); 
			try {
				EFRequest efRq = Common.getEFRequest(rq, rps);
				if(efRq!=null) {
					rps.setRequest(efRq.getParams()); 
					if (Resource.nodeConfig.getSearchConfigs().containsKey(
							efRq.getPipe())) {
						try {
							process(efRq,rps); 
						} catch (Exception e) {
							Common.LOG.error("searcher service http handler error,",e);
							rps.setStatus(e.getMessage(), RESPONSE_STATUS.ParameterErr); 
						}
					} else {
						rps.setStatus(efRq.getPipe()+" alias is not exists or not start up!", RESPONSE_STATUS.ParameterErr); 
					}
				} 
			} catch (Exception e) {
				rps.setStatus(e.getMessage(), RESPONSE_STATUS.ParameterErr); 
			} 
			response.getWriter().println(rps.getResponse(true));
			response.getWriter().flush();
			response.getWriter().close();
		}
	}  
}
