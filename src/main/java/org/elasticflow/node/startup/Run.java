/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.node.startup;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasticflow.computer.service.ComputerService;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.NODE_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.config.NodeConfig;
import org.elasticflow.node.FlowCenter;
import org.elasticflow.node.NodeMonitor;
import org.elasticflow.node.RecoverMonitor;
import org.elasticflow.node.SafeShutDown;
import org.elasticflow.node.SocketCenter;
import org.elasticflow.notifier.EFNotifier;
import org.elasticflow.reader.service.HttpReaderService;
import org.elasticflow.searcher.service.SearcherService;
import org.elasticflow.service.EFMonitorService;
import org.elasticflow.task.FlowTask;
import org.elasticflow.task.schedule.TaskJobCenter;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFIoc;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.ThreadPools;
import org.elasticflow.yarn.coordinator.InstanceCoordinator;
import org.elasticflow.yarn.coordinator.TaskStateCoordinator;
import org.elasticflow.yarn.monitor.ResourceMonitor;
import org.quartz.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 * Application startup position
 * @author chengwen
 * @version 4.0
 * @date 2018-11-19 15:33
 */
public final class Run {
	
	@Autowired
	private Scheduler scheduler; 
	
	@Value("#{nodeSystemInfo['version']}")
	private String version;
	
	@Value("#{nodeSystemInfo['groupId']}")
	private String groupId;	

	private String startConfigPath;

	public Run(String startConfigPath) {
		this.startConfigPath = startConfigPath;
	}
	
	/**
	 * Initialize relevant models and task execution parameters
	 * @param initInstance
	 * @throws EFException 
	 */
	public void init(boolean initInstance) throws EFException {
		
		this.refreshGlobalParam();
		
		Resource.scheduler = scheduler;		
		Resource.EfNotifier = new EFNotifier();
		Resource.tasks = new ConcurrentHashMap<String, FlowTask>();
		Resource.taskJobCenter = new TaskJobCenter();
		Resource.socketCenter =  new SocketCenter();
		Resource.flowCenter = new FlowCenter();
		Resource.nodeMonitor = new NodeMonitor(); 
		Resource.threadPools = new ThreadPools(GlobalParam.STS_THREADPOOL_SIZE);
		
		if(!EFNodeUtil.isSlave()) {//for master
			GlobalParam.TASK_COORDER = new TaskStateCoordinator();
			GlobalParam.INSTANCE_COORDER = new InstanceCoordinator();
		}
		
		int openThreadPools = 0;
		if (initInstance) {
			Resource.nodeConfig = NodeConfig.getInstance(GlobalParam.StartConfig.getProperty("pond"), GlobalParam.StartConfig.getProperty("instructions"));			
			if(EFNodeUtil.isMaster()) {	
				Resource.nodeConfig.init(GlobalParam.StartConfig.getProperty("instances"),
						GlobalParam.StartConfig.getProperty("instances_location"));
				Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
				for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) {
					InstanceConfig instanceConfig = entry.getValue();
					if (instanceConfig.checkStatus())
						EFNodeUtil.loadInstanceDatas(instanceConfig);
					if(instanceConfig.getPipeParams().isMultiThread())
						openThreadPools +=1;
				}
			}
		} 
		
		if(openThreadPools>0 && GlobalParam.DISTRIBUTE_RUN==false) {
			Resource.threadPools.start();
		}
		
		if(EFNodeUtil.isSlave()) {
			EFNodeUtil.initSlaveCoorder();
		}		
	}
	
	private void refreshGlobalParam() {
		GlobalParam.RUN_ENV = String.valueOf(GlobalParam.StartConfig.get("run_environment"));
		GlobalParam.LANG = String.valueOf(GlobalParam.StartConfig.get("language")).toUpperCase();
		GlobalParam.VERSION = version;
		GlobalParam.GROUPID = groupId;
		GlobalParam.DEBUG = GlobalParam.StartConfig.getProperty("is_debug").equals("false") ? false : true;
		GlobalParam.CONNECTION_POOL_SIZE = Integer.parseInt(GlobalParam.StartConfig.getProperty("resource_pool_size"));
		GlobalParam.WRITE_BATCH = GlobalParam.StartConfig.getProperty("write_batch").equals("false") ? false : true;
		GlobalParam.SEND_EMAIL_ON = GlobalParam.StartConfig.getProperty("send_mail").equals("false") ? false : true;
		GlobalParam.SEND_API_ON = GlobalParam.StartConfig.containsKey("send_api")?GlobalParam.StartConfig.getProperty("send_api"):"";
		GlobalParam.DISTRIBUTE_RUN = GlobalParam.StartConfig.getProperty("distribute_run").equals("false") ? false : true;
		GlobalParam.MASTER_HOST = GlobalParam.StartConfig.getProperty("master_host");
		GlobalParam.SERVICE_LEVEL = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());		
		GlobalParam.STS_THREADPOOL_SIZE = Integer.parseInt(GlobalParam.StartConfig.getProperty("sys_threadpool_size"));
		GlobalParam.CLUSTER_MIN_NODES = Integer.parseInt(GlobalParam.StartConfig.getProperty("min_nodes"));
		if(GlobalParam.StartConfig.containsKey("node_ip"))
			GlobalParam.IP = GlobalParam.StartConfig.get("node_ip").toString();
		if(GlobalParam.StartConfig.containsKey("instance_statistics_keep_period"))
			GlobalParam.INSTANCE_STATISTICS_KEEP_PERIOD = Integer.parseInt(GlobalParam.StartConfig.getProperty("instance_statistics_keep_period"));
		
		switch(GlobalParam.StartConfig.getProperty("node_type")) {
		case "master":
			GlobalParam.node_type = NODE_TYPE.master;
			break;
		case "backup":
			GlobalParam.node_type = NODE_TYPE.backup;
			break;
		default:
			GlobalParam.node_type = NODE_TYPE.slave;
		}
	}
	
	/**
	 * distribute on:
	 * 			Master, Monitoring cluster and distribution tasks
	 * 			Slave,  Accept tasks
	 * distribute off:
	 * 			Open all services and run tasks normally
	 */
	public void startService() {
		if(EFNodeUtil.isMaster()) {
			if ((GlobalParam.SERVICE_LEVEL & 1) > 0) {
				Resource.searcherService =  new SearcherService();
				Resource.searcherService.start();
			}			
			if ((GlobalParam.SERVICE_LEVEL & 16) > 0)
				(new ComputerService()).start(); 
			if ((GlobalParam.SERVICE_LEVEL & 8) > 0) {
				Resource.flowCenter.startInstructionsJob(); 
			}				
			new EFMonitorService().start();
		} 		
		if ((GlobalParam.SERVICE_LEVEL & 4) > 0) {
			Resource.httpReaderService = new HttpReaderService();
			Resource.httpReaderService.start();
		}
	}
	
	/**
	 * External loading plug-ins
	 * @param plugin	Jar package storage path 
	 */
	private void loadPlugins(String plugin) {
		if(plugin!=null && plugin.length()>1) {
			List<File> jars = Arrays.asList(new File(plugin).listFiles(new FilenameFilter() {
	            @Override
	            public boolean accept(File dir, String name) {
	                return name.toLowerCase().endsWith(".jar");
	            }
	        }));
			URL[] urls = new URL[jars.size()];
			for (int i = 0; i < jars.size(); i++) {
			    try { 
			        urls[i] = jars.get(i).toURI().toURL();
			    } catch (Exception e) {
			    	Common.LOG.error("load Plugins Exception", e);
			    }
			}
			GlobalParam.PLUGIN_CLASS_LOADER = new URLClassLoader(urls, ClassLoader.getSystemClassLoader());
			Thread.currentThread().setContextClassLoader(GlobalParam.PLUGIN_CLASS_LOADER);  
		}
	}

	private void start() {
		try {
			Runtime.getRuntime().addShutdownHook(new SafeShutDown());
			Common.loadGlobalConfig(this.startConfigPath);
			loadPlugins(GlobalParam.pluginPath);
			GlobalParam.CONFIG_PATH = GlobalParam.StartConfig.getProperty("config_path");							
			if (GlobalParam.StartConfig.get("node_type").equals(NODE_TYPE.backup.name())) {
				init(false);
				new RecoverMonitor().start();
			} else {
				init(true);
				ResourceMonitor.start(); 
				startService();
			} 
			if(GlobalParam.DISTRIBUTE_RUN) {
				Common.LOG.info("ElasticFlow {} {}, nodeID {} Start Success!",GlobalParam.VERSION,
						GlobalParam.StartConfig.get("node_type"),GlobalParam.NODEID);
			}else {
				Common.LOG.info("ElasticFlow {} nodeID {} standalone mode Start Success!",GlobalParam.VERSION,GlobalParam.NODEID);
			}
		} catch (Exception e) {
	    	Common.LOG.error("Init System Exception", e);
	    	Common.stopSystem(false);
	    } 
	} 
	
	/**
	 * System start position
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Resource.EFLOWS = (Run) EFIoc.getBean("EFLOWS");
		Resource.EFLOWS.start(); 
	}
}