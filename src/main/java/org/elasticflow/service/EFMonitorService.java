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
import org.elasticflow.model.EFRequest;
import org.elasticflow.model.EFResponse;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.yarn.Resource;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

/**
 * * Monitor node all instances running
 * 
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
		serviceParams.put("thread_pool", "2");
		serviceParams.put("httpHandle", new httpHandle());
		try {
			HttpService.getInstance(serviceParams).start();
			Common.LOG.info("Management API Service, http://{}:8617",GlobalParam.IP);
		} catch (EFException e) {
			Common.stopSystem(false);
		}
	}

	public class httpHandle extends AbstractHandler {

		@Override
		public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch)
				throws IOException, ServletException {
			response.setStatus(HttpServletResponse.SC_OK);
			response.setHeader("Access-Control-Allow-Origin", "http://"+GlobalParam.PROXY_IP+":8616");
			response.setHeader("Access-Control-Allow-Methods", "DELETE, POST, GET, OPTIONS");
			response.setHeader("Access-Control-Max-Age", "3600");
			response.setHeader("Access-Control-Allow-Headers", "content-security-policy,X-Requested-With,content-type,x-content-type-options,x-xss-protection");
			response.setHeader("Access-Control-Allow-Credentials", "true");
			response.setHeader("Server", "EF");
			Request rq = (request instanceof Request) ? (Request) request
					: HttpConnection.getCurrentConnection().getRequest();
			String dataTo = rq.getPathInfo().substring(1);
			EFResponse rps = EFResponse.getInstance();
			EFRequest EFR = Common.getEFRequest(rq, rps);
			rps.setRequest(EFR.getParams());
			switch (dataTo) {
			case "efm.doaction": {
				Resource.nodeMonitor.ac(rq, EFR, rps);
			}
				break;
			case "_version":
				rps.setInfo(GlobalParam.VERSION);
				break;
			default:
				HashMap<String, Object> info = new HashMap<>();
				info.put("Method", new String[] { "/efm.doaction", "/_version" });
				info.put("Example", " /efm.doaction?ac=getInstanceInfo&instance=demo");
				rps.setPayload(info);
				break;
			}
			if (EFR.getParam("_showtype") != null
					&& EFR.getParam("_showtype").toString().toLowerCase().equals("html")) {
				response.setContentType("text/plain;charset=utf8");
			} else {
				response.setContentType("application/json;charset=utf8");
			}
			response.getWriter().println(rps.getResponse(true));
			response.getWriter().flush();
			response.getWriter().close();
		}
	}
}
