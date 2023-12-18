/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.service;

import java.util.HashMap;

import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.HandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide HTTP Service
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:23
 */
public class HttpService implements EFService {
	
	private boolean status;
	
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
		HandlerCollection handlers = new HandlerCollection();
		handlers.setHandlers(new Handler[] {handler});
		server.setHandler(handlers); 
		server.setStopAtShutdown(true);
		server.setSendServerVersion(true);
	}

	@Override
	public void close() {
		try {
			this.status = false;
			server.stop();
		} catch (Exception e) {
			log.error("close http service exception,",e);
		}
	}

	@Override
	public void start() throws EFException{
		try {
			server.start(); 
			this.status = true;
		} catch (Exception e) {
			this.status = false;
			log.error("start http service exception,",e);
			throw new EFException(e.getMessage(), ELEVEL.Stop);
		} 
	}

	@Override
	public boolean status() {		
		return this.status;
	} 
}
