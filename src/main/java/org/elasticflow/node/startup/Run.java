/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.node.startup;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.elasticflow.computer.service.ComputerService;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.NODE_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.config.NodeConfig;
import org.elasticflow.correspond.ReportStatus;
import org.elasticflow.node.FlowCenter;
import org.elasticflow.node.NodeMonitor;
import org.elasticflow.node.RecoverMonitor;
import org.elasticflow.node.SocketCenter;
import org.elasticflow.reader.service.HttpReaderService;
import org.elasticflow.searcher.service.SearcherService;
import org.elasticflow.service.EFMonitor;
import org.elasticflow.task.FlowTask;
import org.elasticflow.util.Common;
import org.elasticflow.util.ConfigStorer;
import org.elasticflow.util.EFLoc;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.util.ZKUtil;
import org.elasticflow.util.email.EFEmailSender;
import org.elasticflow.yarn.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * Application startup position
 * @author chengwen
 * @version 4.0
 * @date 2018-11-19 15:33
 */
public final class Run {
	@Autowired
	private SearcherService searcherService;
	@Autowired
	private ComputerService computerService;
	@Autowired
	private FlowCenter flowCenter;
	@Autowired
	private RecoverMonitor recoverMonitor;
	@Autowired
	private HttpReaderService httpReaderService;
	@Autowired
	private SocketCenter socketCenter;

	@Value("#{nodeSystemInfo['version']}")
	private String version;
	
	@Value("#{nodeSystemInfo['groupId']}")
	private String groupId;
	
	@Value("#{nodeSystemInfo['debug']}")
	private boolean debug;

	@Autowired
	private EFEmailSender mailSender;

	@Autowired
	NodeMonitor nodeMonitor;

	private String startConfigPath;

	public Run() {

	}

	public Run(String startConfigPath) {
		this.startConfigPath = startConfigPath;
	}

	public void init(boolean initInstance) {
		GlobalParam.RUN_ENV = String.valueOf(GlobalParam.StartConfig.get("run_environment"));
		GlobalParam.VERSION = version;
		GlobalParam.GROUPID = groupId;
		GlobalParam.DEBUG = debug;
		GlobalParam.POOL_SIZE = Integer.parseInt(GlobalParam.StartConfig.getProperty("pool_size"));
		GlobalParam.WRITE_BATCH = GlobalParam.StartConfig.getProperty("write_batch").equals("false") ? false : true;
		GlobalParam.SEND_EMAIL = GlobalParam.StartConfig.getProperty("send_mail").equals("false") ? false : true;
		GlobalParam.SERVICE_LEVEL = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
	
		Resource.mailSender = mailSender;
		Resource.tasks = new HashMap<String, FlowTask>();
		Resource.SOCKET_CENTER = socketCenter;
		Resource.FlOW_CENTER = flowCenter;
		Resource.nodeMonitor = nodeMonitor; 
		
		if (initInstance) {
			ConfigStorer.setData(GlobalParam.CONFIG_PATH + "/EF_NODES/" + GlobalParam.IP + "/configs", JSON.toJSONString(GlobalParam.StartConfig)); 
			Resource.nodeConfig = NodeConfig.getInstance(GlobalParam.StartConfig.getProperty("pond"), GlobalParam.StartConfig.getProperty("instructions"));
			Resource.nodeConfig.init(GlobalParam.StartConfig.getProperty("instances"),GlobalParam.SERVICE_LEVEL);
			Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
			for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) {
				InstanceConfig instanceConfig = entry.getValue();
				if (instanceConfig.checkStatus())
					EFNodeUtil.initParams(instanceConfig);
			}
		}
	}

	public void startService() {
		if ((GlobalParam.SERVICE_LEVEL & 1) > 0) 
			searcherService.start(); 
		
		if ((GlobalParam.SERVICE_LEVEL & 2) > 0) {
			Resource.ThreadPools.start();
			Resource.FlOW_CENTER.buildRWFlow();
		} 
		
		if ((GlobalParam.SERVICE_LEVEL & 4) > 0)
			httpReaderService.start();
		
		if ((GlobalParam.SERVICE_LEVEL & 16) > 0)
			computerService.start(); 
		
		if ((GlobalParam.SERVICE_LEVEL & 8) > 0)
			Resource.FlOW_CENTER.startInstructionsJob();
		
		new EFMonitor().start();
	}

	public void loadGlobalConfig(String path, boolean fromZk) {
		try {
			GlobalParam.StartConfig = new Properties();
			if (fromZk) {
				JSONObject _JO = (JSONObject) JSON.parse(ConfigStorer.getData(path, false));
				for (Map.Entry<String, Object> row : _JO.entrySet()) {
					GlobalParam.StartConfig.setProperty(row.getKey(), String.valueOf(row.getValue()));
				}
			} else {
				String replaceStr = System.getProperties().getProperty("os.name").toUpperCase().indexOf("WINDOWS") == -1
						? "file:"
						: "file:/";
				try (FileInputStream in = new FileInputStream(path.replace(replaceStr, ""))) {
					GlobalParam.StartConfig.load(in);
				} catch (Exception e) {
					Common.LOG.error("load Global Properties file Exception", e);
				}
			}
		} catch (Exception e) { 
			Common.LOG.error("load Global Properties Config Exception", e);
			Common.stopSystem();
		}
		GlobalParam.CONFIG_PATH = GlobalParam.StartConfig.getProperty("config_path");
		GlobalParam.USE_ZK = Boolean.valueOf(GlobalParam.StartConfig.getProperty("use_zk"));
		GlobalParam.INSTANCE_PATH = (GlobalParam.CONFIG_PATH+"/INSTANCES").intern();
		ZKUtil.setZkHost(GlobalParam.StartConfig.getProperty("zkhost"));
	}
	
	private void loadPlugins(String plugin) {
		List<File> jars = Arrays.asList(new File(plugin).listFiles());
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
	
	private void start() {
		loadGlobalConfig(this.startConfigPath, false);
		loadPlugins(GlobalParam.pluginPath);
		GlobalParam.CONFIG_PATH = GlobalParam.StartConfig.getProperty("config_path");		
		ReportStatus.nodeConfigs();
		if (!GlobalParam.StartConfig.containsKey("node_type"))
			GlobalParam.StartConfig.setProperty("node_type", NODE_TYPE.slave.name());
		if (GlobalParam.StartConfig.get("node_type").equals(NODE_TYPE.backup.name())) {
			init(false);
			recoverMonitor.start();
		} else {
			init(true);
			startService();
		}
		Common.LOG.info("ElasticFlow Start Success!");
	} 

	public static void main(String[] args) throws Exception {
		Resource.EFLOWS = (Run) EFLoc.getBean("EFLOWS");
		Resource.EFLOWS.start();
	}

}