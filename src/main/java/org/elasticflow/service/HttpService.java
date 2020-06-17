package org.elasticflow.service;

import java.util.HashMap;

import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.HandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * open http port
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:23
 */
public class HttpService implements EFService {

	private HashMap<String, Object> serviceParams;

	private Server server;
	
	private final static Logger log = LoggerFactory
			.getLogger(HttpService.class);

	public static EFService getInstance(HashMap<String, Object> serviceParams){
		EFService s = new HttpService();
		s.init(serviceParams);
		return s;
	}
	
	@Override
	public void init(HashMap<String, Object> params) {
		this.serviceParams = params;
		server = new Server();
		QueuedThreadPool threadPool = new QueuedThreadPool();
		threadPool.setMaxThreads(Integer.valueOf((String) serviceParams
				.get("thread_pool")));
		threadPool.setMaxIdleTimeMs(30000);
		server.setThreadPool(threadPool);
		
		SelectChannelConnector channelConnector = new SelectChannelConnector();
		channelConnector.setPort(Integer.valueOf(String.valueOf(this.serviceParams
				.get("port"))));
		channelConnector.setMaxIdleTime(Integer.valueOf(String.valueOf(this.serviceParams
				.get("max_idle_time"))));
		channelConnector.setConfidentialPort(Integer
				.valueOf(String.valueOf(this.serviceParams
						.get("confident_port")))); 
		server.addConnector(channelConnector);

		Handler handler = (Handler) this.serviceParams.get("httpHandle");
		HandlerCollection handlerC = new HandlerCollection();
		handlerC.setHandlers(new Handler[] { handler });
		server.setHandler(handlerC);

		server.setStopAtShutdown(true);
		server.setSendServerVersion(true);
	}

	@Override
	public void close() {
		try {
			server.stop();
		} catch (Exception e) {
			log.error("close Exception,",e);
		}
	}

	@Override
	public void start() {
		try {
			server.start(); 
		} catch (Exception e) {
			log.error("start Exception,",e);
		} 
	} 
}
