/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.config;

import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Properties;

import org.elasticflow.model.FormatProperties;
import org.elasticflow.yarn.coord.DiscoveryCoord;
import org.elasticflow.yarn.coord.InstanceCoord;
import org.elasticflow.yarn.coord.TaskStateCoord;

/**
 * global node data store position
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public final class GlobalParam {

	/** ------------system global information------------ */
	public static final String PROJ = "ElasticFlow";

	public static long SYS_START_TIME = System.currentTimeMillis();

	public static final String ENCODING = "utf-8";
	
	/** ------------system Runtime Environment------------ */
	public static boolean DEBUG;
	
	public static String LANG = "EN";
	
	public static enum ELEVEL {
		Ignore, Dispose,BreakOff,Termination, Stop;
	}

	public static enum ETYPE {
		RESOURCE_ERROR, PARAMETER_ERROR, DATA_ERROR, EXTINTERRUPT, UNKNOWN;
	} 
	
	/** ----- flow settting -----*/
	/**statistics storage maximum time period**/
	public static int INSTANCE_STATISTICS_KEEP_PERIOD = 7;
	
	/** error notify setting */
	public static boolean SEND_EMAIL_ON = false;
	public static String SEND_API_ON;

	/** system run environment alpha beta or product... */
	public static String RUN_ENV;

	public static String VERSION;
	
    /**Package and class prefix identification*/
	public static String GROUPID;
	
	/**The maximum number of connections that can be created by default for each source*/
	public static int CONNECTION_POOL_SIZE = 6;
	
	/**Whether to enable batch writing of identifiers depends on the type of writing end*/
	public static boolean WRITE_BATCH = false;
	/**
	 * #1 searcher service 2 writer service 4 http reader service 8 instruction
	 * service 16 compute service
	 */
	public static int SERVICE_LEVEL; 
	
	/** node ip **/
	public static String IP = "127.0.0.1";
	/**proxy ip **/
	public static String PROXY_IP = "127.0.0.1";
	
	/**Computer parameters*/
	public final static String MAIN_PY = "entrance";

	/** CONNECT_EXPIRED is milliseconds time */
	public static int CONNECT_EXPIRED = 7200000;

	public static Properties SystemConfig = new FormatProperties();

	/** configure file local path */
	public static String CONFIG_ROOT = System.getProperty("config"); 
	
	public static String SYS_CONFIG_PATH = CONFIG_ROOT+"/config";

	public static String DATAS_CONFIG_PATH = CONFIG_ROOT+"/datas";
	
	public static String RESTART_SHELL_COMMAND = "nohup "+CONFIG_ROOT+"/restart.sh"+" > /dev/null 2&1";

	public static String INSTANCE_PATH = (DATAS_CONFIG_PATH+"/INSTANCES").intern();

	/** configure plugin local path */
	public static final String pluginPath = System.getProperty("plugin");
	public static volatile URLClassLoader PLUGIN_CLASS_LOADER;

	/** ------------system distribute running configure------------ */
	public static int MASTER_SYN_PORT = 8618;

	public static int SLAVE_SYN_PORT = 8619;

	public static final long NODE_LIVE_TIME = 6000;

	public static int CLUSTER_MIN_NODES;
	
	/**Machine node identification*/
	public final static int NODEID = Integer.valueOf(System.getProperty("nodeid"));
	
	public static int STS_THREADPOOL_SIZE = 100;

	public static String MASTER_HOST = "";

	public static NODE_TYPE node_type;

	public static boolean DISTRIBUTE_RUN = false;

	public static TaskStateCoord TASK_COORDER;

	public static InstanceCoord INSTANCE_COORDER;

	public static DiscoveryCoord DISCOVERY_COORDER;

	/** master,slave,backup */
	public static enum NODE_TYPE {
		master, slave, backup
	};

	/** ------------instance parameters------------ */
	public static enum END_TYPE {
		reader, computer, writer, searcher
	} 
			
	public static enum RESOURCE_TYPE {
		WAREHOUSE, INSTRUCTION
	}

	/** Task Running SINGAL define */
	public static enum TASK_FLOW_SINGAL {
		Blank(0), Ready(1), Running(2), Termination(4), Stop(8), Waiting(16);

		private int v;

		private TASK_FLOW_SINGAL(int val) {
			this.v = val;
		}

		public int getVal() {
			return v;
		}
	}
	
	/** resource status define**/
	public static enum RESOURCE_STATUS {
		Normal(0), Warning(1), Error(2);

		private int v;

		private RESOURCE_STATUS(int val) {
			this.v = val;
		}

		public int getVal() {
			return v;
		}
	}
	
	/** instance status define**/
	public static enum INSTANCE_STATUS {
		Normal(0), Warning(1), Error(2);

		private int v;

		private INSTANCE_STATUS(int val) {
			this.v = val;
		}

		public int getVal() {
			return v;
		}
	}
	public static enum DATA_SOURCE_TYPE {
		MYSQL, ORACLE, HIVE, ES, HBASE, UNKNOWN, H2, FILES, NEO4J, KAFKA, VEARCH, HDFS, FASTDFS,ROCKETMQ
	}

	public static enum FLOW_TAG {
		_DEFAULT, _MOP
	}

	public final static String DEFAULT_FIELD = "SYSTEM_UPDATE_TIME";
	public final static String DEFAULT_RESOURCE_SEQ = "";

	public static enum JOB_TYPE {
		VIRTUAL, FULL, INCREMENT, OPTIMIZE, INSTRUCTION
	}

	public static enum FLOWINFO {
		FULL_STATE, FULL_STOREID, INCRE_STOREID, FULL_JOBS
	}

	public final static String DEFAULT_SEQ = "__";
	public final static String JOB_INCREMENTINFO_PATH = "increment";
	public final static String JOB_FULLINFO_PATH = "full";

	/**
	 * writer parameters Mechanism: AB Active/standby switching mode, Time Build
	 * instance in time steps, NORM Keep only a single instance mode
	 */
	public static enum MECHANISM {
		AB, Time, NORM
	}

	public static enum INSTANCE_TYPE {
		Blank(0), Trans(1), WithCompute(2);

		private int v;

		private INSTANCE_TYPE(int val) {
			this.v = val;
		}

		public int getVal() {
			return v;
		}
	}

	/** ------------External API call feedback Response status define------------ */
	public static enum RESPONSE_STATUS {
		Success(0), DataErr(100), CodeException(200), ParameterErr(300), Unknown(400), ExternErr(500);

		private int v;
		private static HashMap<Integer, String> MSG = new HashMap<Integer, String>() {
			private static final long serialVersionUID = -3931502298193106809L;
			{
				put(0, "success");
				put(100, "Runtime data error！");
				put(200, "System Code Running Exception!");
				put(300, "Parameter exception!");
				put(400, "Unknown exception!");
				put(500, "External exception!");
			}
		};

		private RESPONSE_STATUS(int val) {
			this.v = val;
		}

		public int getVal() {
			return this.v;
		}

		public String getMsg() {
			return MSG.get(this.v);
		}
	}

	public static enum FIELD_PARSE_TYPE {
		valueOf, parse
	}

	public static enum KEY_PARAM {
		start, count, sort, facet, detail, facet_count, group, fl, __storeid
	}

	/** ------------computer parameters------------ */
	public static enum COMPUTER_MODE {
		REST,MODEL,PY,BLANK
	}

	/** ------------searcher parameters------------ */
	public final static String INSATANCE_STAT = "__stats";
	public final static String CUSTOM_QUERY = "custom_query";
	public final static String CLOSE_REQUEST_RESPONSE = "__request_not_return";
	public final static int SEARCH_MAX_WINDOW = 20000;
	public final static int SEARCH_MAX_PAGE = 2000;
	public final static float DISJUNCTION_QUERY_WEIGHT = 0.1f;
	public final static int FACET_DEFAULT_COUNT = 200;
	public final static int FACET_DEAULT_SHOW_COUNT = 3;
	public final static String SORT_ASC = "_asc";
	public final static String SORT_DESC = "_desc";
	public final static String PARAM_FL = "fl";

	public final static String PARAM_POST_FILTER = "post_filter";
	public final static String PARAM_FUZZY = "fuzzy";
	public final static String PARAM_KEYWORD = "keyword";
	public final static String PARAM_GROUP = "group";
	public final static String PARAM_ANDSCRIPT = "ANDscript";
	public final static String PARAM_ORSCRIPT = "ORscript";
	public final static String PARAM_SCRIPT_TYPE = "script_type";
	public final static String PARAM_DEFINEDSEARCH = "search_dsl";
	public final static String PARAM_SHOWQUERY = "showquery";
	public final static String PARAM_FIELD_SCORE = "field_score";
	public final static String PARAM_FIELD_RANDOM = "field_random";
	public final static String PARAM_FACET_ORIGINAL = "facet_original";
	public final static String PARAM_FACET = "facet";
	public final static String PARAM_FACET_EXT = "facet_ext";
	public final static String PARAM_SORT = "sort";
	public final static String NOT_SUFFIX = "_not";
	public final static String PARAM_REQUEST_HANDLER = "request_handler";

	public static enum QUERY_TYPE {
		BOOLEAN_QUERY, DISJUNCTION_QUERY
	}
	
	/**searcher parameters of response*/
	public final static String RESPONSE_SCORE = "__SCORE";
	public final static String RESPONSE_EXPLAINS = "__EXPLAINS";
	
	/** ------------reader data scan parameters------------ */
	public static final String _start = "#{page_start}";
	public static final String _end = "#{page_end}";
	public static final String _seq = "#{seq}";
	public static final int READ_PAGE_SIZE = 10000;
	public static final String _scan_field = "#{scan_field}";
	public static final String _page_field = "#{page_field}";
	public static final String _start_time = "#{start_time}";
	public static final String _end_time = "#{end_time}";
	public static final String READER_KEY = "KeyField";
	public static final String READER_PAGE_KEY = "_PAGE_FIELD";
	public static final String READER_SCAN_KEY = "_SCAN_FIELD";
	public static final String READER_LAST_STAMP = "lastUpdateTime";
	public static final String READER_STATUS = "_reader_status";
}