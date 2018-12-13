package org.elasticflow.service;

import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.MD5Util;
import org.elasticflow.yarn.Resource;


/**
 *  * Monitor node all instances running
 * @author chengwen
 * @version 4.0
 * @date 2018-10-26 09:13
 */
public class FNMonitor {  
	
	public void start() {
		HashMap<String, Object> serviceParams = new HashMap<String, Object>();
		serviceParams.put("confident_port", "8601");
		serviceParams.put("max_idle_time", "20000");
		serviceParams.put("port", "8617");
		serviceParams.put("thread_pool", "3");
		serviceParams.put("httpHandle", new httpHandle());
		HttpService.getInstance(serviceParams).start();		
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

			switch (dataTo) {  
			case "search.doaction":{
				if(rq.getParameter("ac") !=null && rq.getParameter("code")!=null && rq.getParameter("code").equals(MD5Util.SaltMd5(rq.getParameter("ac")))){
					Resource.nodeMonitor.ac(rq);
					response.getWriter().println(Resource.nodeMonitor.getResponse()); 
					Resource.nodeMonitor.setResponse(0, "");
				}else{
					response.getWriter().println("{\"status\":0,\"info\":\"Action failed!parameter ac or code error!\"}");
				}
			}
				break;   
			case "_version":
				response.getWriter().println(GlobalParam.VERSION);
				break;
			}
			response.getWriter().flush();
			response.getWriter().close();
		}
	}  
}
