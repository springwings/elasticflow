/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang.time.FastDateFormat;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.FIELD_PARSE_TYPE;
import org.elasticflow.config.GlobalParam.KEY_PARAM;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.EFSearchRequest;
import org.elasticflow.model.FormatProperties;
import org.elasticflow.model.InstructionTree;
import org.elasticflow.model.NMRequest;
import org.elasticflow.node.SafeShutDown;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.yarn.Resource;
import org.mortbay.jetty.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.alibaba.fastjson.JSONObject;

/**
 * Common Utils Package
 * 
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */
public final class Common {

	public static FastDateFormat SDF = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

	public final static Logger LOG = LoggerFactory.getLogger("Elasticflow");

	private static Set<String> defaultParamSet = new HashSet<String>() {
		private static final long serialVersionUID = 1L;
		{
			add(KEY_PARAM.start.toString());
			add(KEY_PARAM.count.toString());
			add(KEY_PARAM.fl.toString());
			add(KEY_PARAM.facet.toString());
			add(KEY_PARAM.sort.toString());
			add(KEY_PARAM.group.toString());
			add(KEY_PARAM.facet_count.toString());
			add(KEY_PARAM.detail.toString());
		}
	};

	/**
	 * Convert the first letter to uppercase or lowercase
	 * 
	 * @param str
	 * @return
	 */
	public static String changeFirstCase(String str) {
		char[] chars = str.toCharArray();
		chars[0] ^= 32;
		return String.valueOf(chars);
	}

	public static boolean isDefaultParam(String p) {
		if (defaultParamSet.contains(p))
			return true;
		else
			return false;
	}

	public static FormatProperties loadProperties(String path) {
		FormatProperties fp = new FormatProperties();
		String replaceStr = System.getProperties().getProperty("os.name").toUpperCase().indexOf("WINDOWS") == -1
				? "file:"
				: "file:/";
		try (FileInputStream in = new FileInputStream(path.replace(replaceStr, ""))) {
			fp.load(in);
		} catch (Exception e) {
			Common.LOG.error("load Global Properties file Exception", e);
		}
		return fp;
	}

	public static void getXmlParam(Object Obj, Node param, Class<?> c) throws Exception {
		Element element = (Element) param;
		setConfigObj(Obj, c, element.getElementsByTagName("name").item(0).getTextContent(),
				element.getElementsByTagName("value").item(0).getTextContent());
	}

	public static String formatXml(Object xmlContent) {
		try {
			OutputFormat formater = OutputFormat.createPrettyPrint();
			formater.setEncoding("utf-8");
			StringWriter out = new StringWriter();
			XMLWriter writer = new XMLWriter(out, formater);
			writer.write(xmlContent);
			writer.close();
			return out.toString();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static Object getXmlObj(Node param, Class<?> c) throws Exception {
		Element element = (Element) param;
		Constructor<?> cons = c.getConstructor();
		Object o = cons.newInstance();
		Field[] fields = c.getDeclaredFields();
		for (int f = 0; f < fields.length; f++) {
			Field field = fields[f];
			String value = null;
			String fieldName = field.getName();
			NodeList list = element.getElementsByTagName(fieldName);

			if (list != null && list.getLength() > 0) {
				Node node = list.item(0);
				value = node.getTextContent();
			} else {
				value = element.getAttribute(fieldName);
			}

			if (param.getNodeName().equals(fieldName)) {
				value = param.getTextContent();
			}
			setConfigObj(o, c, fieldName, value);
		}
		return o;
	}

	@SuppressWarnings("unchecked")
	public static void setConfigObj(Object Obj, Class<?> c, String fieldName, String value) throws Exception {
		if (Obj instanceof HashMap) {
			((HashMap<String, String>) Obj).put(fieldName, value);
		} else {
			if (value != null && value.length() > 0) {
				String setMethodName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
				Method setMethod = c.getMethod(setMethodName, new Class[] { String.class });
				setMethod.invoke(Obj, new Object[] { value });
			}
		}
	}

	/**
	 * * Second timestamp of now
	 * 
	 * @return
	 */
	public static long getNow() {
		return System.currentTimeMillis() / 1000;
	}

	/**
	 * Second timestamp of the current zero point
	 * 
	 * @return
	 */
	public static long getNowZero() {
		return (System.currentTimeMillis() / (1000 * 3600 * 24) * (1000 * 3600 * 24)
				- TimeZone.getDefault().getRawOffset()) / 1000;
	}

	public static String seconds2time(long second) {
		long h = 0;
		long m = 0;
		long s = 0;
		long temp = second % 3600;
		if (second > 3600) {
			h = second / 3600;
			if (temp != 0) {
				if (temp > 60) {
					m = temp / 60;
					if (temp % 60 != 0) {
						s = temp % 60;
					}
				} else {
					s = temp;
				}
			}
		} else {
			m = second / 60;
			if (second % 60 != 0) {
				s = second % 60;
			}
		}

		String ret = "";
		if (h >= 10)
			ret += h + "h";
		else if (h > 0)
			ret += "0" + h + "h";

		if (m >= 10)
			ret += m + "m";
		else if (m > 0)
			ret += "0" + m + "m";

		if (s >= 10)
			ret += s + "s";
		else if (s >= 0)
			ret += "0" + s + "s";

		return ret;
	}

	public static List<String> stringToList(String str, String seperator) {
		return Arrays.asList(str.split(seperator));
	}

	public static String arrayToString(String[] strs, String seperator) {
		StringBuilder sf = new StringBuilder();
		if (strs.length > 0) {
			for (String s : strs) {
				sf.append(",");
				sf.append(s);
			}
			return sf.substring(1);
		}
		return sf.toString();
	}

	// get hashmap min key
	public static Object getMinKey(Set<String> keySets) {
		Object[] obj = keySets.toArray();
		Arrays.sort(obj);
		return obj[0];
	}

	public static String getLseq(String L1seq, String L2seq) {
		if(L1seq=="" && L2seq=="")
			return "_";
		return L1seq + "." + L2seq;
	}

	/**
	 * @param instanceName data source main tag name
	 * @param storeId      a/b or time mechanism tags
	 * @return String
	 */
	public static String getStoreName(String instanceName, String storeId) {
		if (storeId != null && storeId.length() > 0) {
			return instanceName + "_" + storeId;
		} else {
			return instanceName;
		}

	}
		
	//full each L1seq one file
	public static String getStoreTaskInfo(String instance,boolean isfull) {
		String info = "{}";
		String path = Common.getTaskStorePath(instance, 
				isfull?GlobalParam.JOB_FULLINFO_PATH:GlobalParam.JOB_INCREMENTINFO_PATH);
		byte[] b = EFDataStorer.getData(path, true);
		if (b != null && b.length > 0) {
			String str = new String(b);
			if (str.length() > 1) {
				info = str;
			}
		}
		return info;
	}

	/**
	 * @param L1seq    for data source sequence tag
	 * @param instance data source main tag name
	 * @return String
	 */
	public static String getInstanceRunId(String instance, String L1seq) {
		if (L1seq != null && L1seq.length() > 0) {
			return instance + L1seq;
		} else {
			return instance;
		}
	}

	/**
	 * seq for split data
	 * 
	 * @param indexname
	 * @param L1seq,for series data source fetch
	 * @return
	 */
	public static String getTaskStorePath(String instanceName, String location) {
		return GlobalParam.INSTANCE_PATH + "/" + instanceName + "/" + location;
	}

	public static String getResourceTag(String instance, String L1seq, String tag, boolean ignoreSeq) {
		StringBuilder tags = new StringBuilder();
		if (!ignoreSeq && L1seq != null && L1seq.length() > 0) {
			tags.append(instance).append(L1seq);
		} else {
			tags.append(instance).append(GlobalParam.DEFAULT_RESOURCE_SEQ);
		}
		return tags.append(tag).toString();
	}

	/**
	 * get read data source seq flags
	 * 
	 * @param instanceName
	 * @param fillDefault  if empty fill with system default blank seq
	 * @return
	 */
	public static String[] getL1seqs(InstanceConfig instanceConfig) {
		String[] seqs = {};
		WarehouseParam wp = Resource.nodeConfig.getWarehouse().get(instanceConfig.getPipeParams().getReadFrom());
		if (null != wp) {
			seqs = wp.getL1seq();
		} else {
			LOG.warn("{} resource is null.", instanceConfig.getPipeParams().getReadFrom());
		}
		return seqs;
	}

	/**
	 * 
	 * @param heads
	 * @param instanceName
	 * @param storeId
	 * @param seq            table seq
	 * @param total
	 * @param dataBoundary
	 * @param lastUpdateTime
	 * @param useTime
	 * @param types
	 * @param moreinfo
	 */

	public static String formatLog(String types, String heads, String instanceName, String storeId, String L2seq,
			int total, String dataBoundary, String lastUpdateTime, long useTime, String moreinfo) {
		String useTimeFormat = Common.seconds2time(useTime);
		if (L2seq.length() < 1)
			L2seq = "None";
		String update;
		if (lastUpdateTime.length() > 9 && lastUpdateTime.matches("[0-9]+")) {
			update = SDF.format(
					lastUpdateTime.length() < 12 ? Long.valueOf(lastUpdateTime + "000") : Long.valueOf(lastUpdateTime));
		} else {
			update = lastUpdateTime;
		}
		StringBuilder sb = new StringBuilder();
		switch (types) {
		case "complete":
			sb.append("[Complete " + heads + " " + instanceName + "_" + storeId + "] " + (" L2seq:" + L2seq));
			sb.append(" Docs:" + total);
			sb.append(" scanAt:" + update);
			sb.append(" useTime:" + useTimeFormat);
			break;
		case "start":
			sb.append("[Start " + heads + " " + instanceName + "_" + storeId + "] " + (" L2seq:" + L2seq));
			sb.append(" scanAt:" + update);
			break;
		default:
			sb.append("[ -- " + heads + " " + instanceName + "_" + storeId + "] " + (" L2seq:" + L2seq));
			sb.append(
					" Docs:" + total + (total == 0 || dataBoundary.length() < 1 ? "" : " dataBoundary:" + dataBoundary)
							+ " scanAt:" + update + " useTime:" + useTimeFormat);
			break;
		}
		return sb.append(moreinfo).toString();
	}

	public static ArrayList<InstructionTree> compileCodes(String code, String contextId) {
		ArrayList<InstructionTree> res = new ArrayList<>();
		for (String line : code.trim().split("\\n")) {
			InstructionTree instructionTree = null;
			InstructionTree.Node tmp = null;
			if (line.indexOf("//") > -1)
				line = line.substring(0, line.indexOf("//"));
			for (String str : line.trim().split("->")) {
				if (instructionTree == null) {
					instructionTree = new InstructionTree(str, contextId);
					tmp = instructionTree.getRoot();
				} else {
					String[] params = str.trim().split(",");
					for (int i = 0; i < params.length; i++) {
						if (i == params.length - 1) {
							tmp = instructionTree.addNode(params[i], tmp);
						} else {
							instructionTree.addNode(params[i], tmp);
						}
					}

				}
			}
			res.add(instructionTree);
		}
		return res;
	}

	public static Object parseFieldValue(Object v, EFField fd, FIELD_PARSE_TYPE parsetype) throws EFException {
		if (fd == null)
			return v;
		if (v == null) {
			return fd.getDefaultvalue();
		} else {
			Class<?> c;
			try {
				if (fd.getParamtype().startsWith(GlobalParam.GROUPID) || fd.getParamtype().startsWith("java.lang")) {
					c = Class.forName(fd.getParamtype());
				} else {
					c = Class.forName(fd.getParamtype(), true, GlobalParam.PLUGIN_CLASS_LOADER);
				}
				Method method;
				if (fd.getParamtype().equals("java.lang.Double")) {
					method = c.getMethod(parsetype.name(), String.class);
				} else {
					method = c.getMethod(parsetype.name(), Object.class);
				}
				if (fd.getSeparator() != null) {
					String[] vs = String.valueOf(v).split(fd.getSeparator());
					if (!fd.getParamtype().equals("java.lang.String")) {
						Object[] _vs = new Object[vs.length];
						for (int j = 0; j < vs.length; j++)
							_vs[j] = method.invoke(c, vs[j]);
						return _vs;
					}
					return vs;
				} else {
					return method.invoke(c, v);
				}
			} catch (Exception e) {
				throw new EFException(e.getMessage() + ",Field " + fd.getName(), ELEVEL.Dispose);
			}
		}
	}

	public static boolean exceptionCheckContain(Exception ex, String key) {
		StringBuffer sb = new StringBuffer();
		StackTraceElement[] trace = ex.getStackTrace();
		for (StackTraceElement s : trace) {
			sb.append("\tat " + s + "\r\n");
		}
		return sb.toString().contains(key);
	}

	public static EFSearchRequest getEFRequest(Request rq, EFResponse rps) {
		EFSearchRequest RR = null;
		String ctype = rq.getHeader("Content-type");
		if (ctype != null && ctype.contentEquals("application/json")) {
			try (BufferedReader _br = new BufferedReader(new InputStreamReader(rq.getInputStream(), "UTF-8"));) {
				String line = null;
				StringBuilder sb = new StringBuilder();
				while ((line = _br.readLine()) != null) {
					sb.append(line);
				}
				RR = Common.getRequest(sb.toString());
				RR.setPipe(rq.getPathInfo().substring(1));
			} catch (Exception e) {
				rps.setStatus(e.getMessage(), RESPONSE_STATUS.ParameterErr);
			}
		} else {
			RR = Common.getRequest(rq);
		}
		return RR;
	}

	/**
	 * json request convert to EFLOWSRequest
	 * 
	 * @param input
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static EFSearchRequest getRequest(String jsonInput) {
		EFSearchRequest rr = EFSearchRequest.getInstance();
		JSONObject jsonObject = JSONObject.parseObject(jsonInput);
		Iterator<?> iter = jsonObject.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			rr.addParam(entry.getKey().toString(), entry.getValue());
		}
		return rr;
	}

	/**
	 * jetty request convert to EFLOWSRequest
	 * 
	 * @param input
	 * @return
	 */
	public static EFSearchRequest getRequest(Request input) {
		EFSearchRequest rr = EFSearchRequest.getInstance();
		Request rq = (Request) input;
		String path = rq.getPathInfo();
		String pipe = path.substring(1);
		rr.setPipe(pipe);
		@SuppressWarnings("unchecked")
		Iterator<Map.Entry<String, String>> iter = rq.getParameterMap().entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, String> entry = iter.next();
			String key = (String) entry.getKey();
			String value = rq.getParameter(key);
			rr.addParam(key, value);
		}
		return rr;
	}

	/**
	 * jetty request convert to Node Monitor Request
	 * 
	 * @param input
	 * @return
	 */
	public static NMRequest getNMRequest(Request input) {
		NMRequest rr = NMRequest.getInstance();
		Request rq = (Request) input;
		@SuppressWarnings("unchecked")
		Iterator<Map.Entry<String, String>> iter = rq.getParameterMap().entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, String> entry = iter.next();
			String key = (String) entry.getKey();
			String value = rq.getParameter(key);
			rr.addParam(key, value);
		}
		return rr;
	}

	/**
	 * Gets the start time stamp of the current month
	 * 
	 * @param timeStamp
	 * @param timeZone  GMT+8:00
	 * @return
	 */
	public static Long getMonthStartTime(Long timeStamp, String timeZone) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TimeZone.getTimeZone(timeZone));
		calendar.setTimeInMillis(timeStamp);
		calendar.add(Calendar.YEAR, 0);
		calendar.add(Calendar.MONTH, 0);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTimeInMillis();
	}

	public static EFException getException(Exception e) {
		Throwable except = e.getCause();
		if (except instanceof EFException) {
			return (EFException) except;
		} else {
			return new EFException(e);
		}
	}

	public static void processErrorLevel(EFException e) {
		if (e.getErrorLevel().equals(ELEVEL.Termination)) {
			LOG.error("The current thread automatically interrupt!",e);
			Resource.EfNotifier.send("The current thread automatically interrupt!", e.getMessage(), false);
			Thread.currentThread().interrupt();
		} else if (e.getErrorLevel().equals(ELEVEL.Stop)) {
			stopSystem(true);
		}
	}

	public static void stopSystem(boolean soft) {
		LOG.warn("system will automatically stop...");
		if (soft)
			SafeShutDown.stopAllInstances();
		LOG.warn("system stop success!");
		Resource.ThreadPools.execute(() -> {
			System.exit(0);
		});
	}

}
