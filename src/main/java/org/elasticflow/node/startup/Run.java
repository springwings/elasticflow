package org.elasticflow.node.startup;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
import org.elasticflow.service.FNMonitor;
import org.elasticflow.task.FlowTask;
import org.elasticflow.util.Common;
import org.elasticflow.util.FNIoc;
import org.elasticflow.util.NodeUtil;
import org.elasticflow.util.ZKUtil;
import org.elasticflow.util.email.FNEmailSender;
import org.elasticflow.yarn.Resource;

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

	@Autowired
	private FNEmailSender mailSender;

	@Autowired
	NodeMonitor nodeMonitor;

	private String startConfigPath;

	public Run() {

	}

	public Run(String startConfigPath) {
		this.startConfigPath = startConfigPath;
	}

	public void init(boolean initInstance) {
		GlobalParam.run_environment = String.valueOf(GlobalParam.StartConfig.get("run_environment"));
		GlobalParam.VERSION = version;
		GlobalParam.POOL_SIZE = Integer.parseInt(GlobalParam.StartConfig.getProperty("pool_size"));
		GlobalParam.WRITE_BATCH = GlobalParam.StartConfig.getProperty("write_batch").equals("false") ? false : true;
		GlobalParam.SERVICE_LEVEL = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
	
		Resource.mailSender = mailSender;
		Resource.tasks = new HashMap<String, FlowTask>();
		Resource.SOCKET_CENTER = socketCenter;
		Resource.FlOW_CENTER = flowCenter;
		Resource.nodeMonitor = nodeMonitor; 
		
		if (initInstance) {
			ZKUtil.setData(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP + "/configs",
					JSON.toJSONString(GlobalParam.StartConfig));
			Resource.nodeConfig = NodeConfig.getInstance(GlobalParam.StartConfig.getProperty("pond"), GlobalParam.StartConfig.getProperty("instructions"));
			Resource.nodeConfig.init(GlobalParam.StartConfig.getProperty("instances"),GlobalParam.SERVICE_LEVEL);
			Map<String, InstanceConfig> configMap = Resource.nodeConfig.getInstanceConfigs();
			for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) {
				InstanceConfig instanceConfig = entry.getValue();
				if (instanceConfig.checkStatus())
					NodeUtil.initParams(instanceConfig);
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
		
		new FNMonitor().start();
	}

	public void loadGlobalConfig(String path, boolean fromZk) {
		try {
			GlobalParam.StartConfig = new Properties();
			if (fromZk) {
				JSONObject _JO = (JSONObject) JSON.parse(ZKUtil.getData(path, false));
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
		}
		GlobalParam.CONFIG_PATH = GlobalParam.StartConfig.getProperty("zkConfigPath");
		GlobalParam.INSTANCE_PATH = (GlobalParam.CONFIG_PATH+"/INSTANCES").intern();
		ZKUtil.setZkHost(GlobalParam.StartConfig.getProperty("zkhost"));
	}

	private void start() {
		loadGlobalConfig(this.startConfigPath, false);
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
		Resource.RIVERS = (Run) FNIoc.getBean("RIVERS");
		Resource.RIVERS.start();
	}

}