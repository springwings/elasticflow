package org.elasticflow.computer.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;
import org.springframework.beans.factory.annotation.Autowired;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.EFRequest;
import org.elasticflow.node.SocketCenter;
import org.elasticflow.service.EFService;
import org.elasticflow.service.HttpService;
import org.elasticflow.util.Common;
import org.elasticflow.yarn.Resource;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-31 14:46
 */
public class ComputerService {
	
	private EFService FS;
	
	@Autowired
	private SocketCenter SocketCenter;    
	
	
	public boolean start() {
		HashMap<String, Object> serviceParams = new HashMap<String, Object>();
		serviceParams.put("confident_port", GlobalParam.StartConfig.get("computer_service_confident_port"));
		serviceParams.put("max_idle_time", GlobalParam.StartConfig.get("computer_service_max_idle_time"));
		serviceParams.put("port", GlobalParam.StartConfig.get("computer_service_port"));
		serviceParams.put("thread_pool", GlobalParam.StartConfig.get("computer_service_thread_pool"));
		serviceParams.put("httpHandle", new httpHandle());
		FS=HttpService.getInstance(serviceParams);		
		FS.start();
		return true;
	}
	public boolean close(){
		if(FS!=null){
			FS.close();
		} 
		return true;
	}
	public EFResponse process(EFRequest request) { 
		long startTime = System.currentTimeMillis();
		EFResponse response = null; 
		String pipe = request.getPipe(); 
		Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
		if (configMap.containsKey(pipe)) { 
			response = SocketCenter.getComputer(pipe,"","",false).startCompute(request);
		} 
		long endTime = System.currentTimeMillis();
		if (response != null){
			response.setStartTime(startTime); 
			response.setEndTime(endTime);
		}
		return response;
	}
	
	
	class httpHandle extends AbstractHandler { 
		@Override
		public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch)
				throws IOException, ServletException {
			Request rq = (request instanceof Request) ? (Request) request
					: HttpConnection.getCurrentConnection().getRequest();

			response.setContentType("application/json;charset=utf8");
			response.setStatus(HttpServletResponse.SC_OK);
			response.setHeader("PowerBy", GlobalParam.PROJ); 
			rq.setHandled(true);
			EFRequest RR = Common.getRequest(rq);
			EFResponse rps = EFResponse.getInstance();
			rps.setRequest(RR.getParams());
			if (Resource.nodeConfig.getSearchConfigs().containsKey(
					RR.getPipe())) {
				try {
					rps = process(RR);
					response.getWriter().println(rps.getResponse(true));
				} catch (Exception e) {
					Common.LOG.error("Computer http handler error,",e);
					rps.setStatus("Computer http handler error!", RESPONSE_STATUS.CodeException);
					response.getWriter().println(rps.getResponse(true));
				}
			} else {
				rps.setStatus("The Computer Service not exists or not Start Up!", RESPONSE_STATUS.CodeException);
				response.getWriter().println(rps.getResponse(true));
			}
			response.getWriter().flush();
			response.getWriter().close();
		} 
	}
}
