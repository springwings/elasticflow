/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.config;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.elasticflow.config.GlobalParam.INSTANCE_TYPE;
import org.elasticflow.config.GlobalParam.RESOURCE_TYPE;
import org.elasticflow.param.pipe.InstructionParam;
import org.elasticflow.param.warehouse.WarehouseNosqlParam;
import org.elasticflow.param.warehouse.WarehouseSqlParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.instance.EFDataStorer;

/**
 * The EF node configuration control center,
 * manages the abstract flow and data source configuration.
 * 
 * @author chengwen
 * @version 4.1
 * @date 2018-10-11 14:50
 */
public class NodeConfig {

	private final Map<String, InstanceConfig> instanceConfigs = new HashMap<>();
	private final Map<String, InstanceConfig> searchConfigMap = new HashMap<>();
	private final Map<String, WarehouseSqlParam> sqlWarehouse = new HashMap<>();
	private final Map<String, WarehouseNosqlParam> NoSqlWarehouse = new HashMap<>();
	private final Map<String, InstructionParam> instructions = new HashMap<>();
	private String pondFile = null;
	private String instructionsFile = null;

	/**
	 * 
	 * @param instances
	 * @param pondFile
	 * @param instructionsFile
	 * @return
	 */
	public static NodeConfig getInstance(String pondFile, String instructionsFile) {
		NodeConfig o = new NodeConfig();
		o.pondFile = pondFile;
		o.instructionsFile = instructionsFile;
		return o;
	}

	public void init(String instances, int SERVICE_LEVEL) {
		loadConfig(instances, true);
		if ((SERVICE_LEVEL & 1) > 0) {
			for (Map.Entry<String, InstanceConfig> e : this.instanceConfigs.entrySet()) {
				this.searchConfigMap.put(e.getValue().getAlias(), e.getValue());
			}
		} 
	}

	public void loadConfig(String instances, boolean reset) {
		if (reset) {
			this.instanceConfigs.clear();
			parsePondFile(GlobalParam.CONFIG_PATH + "/" + this.pondFile);
			parseInstructionsFile(GlobalParam.CONFIG_PATH + "/" + this.instructionsFile);
		}
		if (instances.trim().length() < 1)
			return;
		for (String inst : instances.split(",")) {
			String[] strs = inst.split(":");
			if (strs.length <= 0 || strs[0].length() < 1)
				continue;
			int instanceType = INSTANCE_TYPE.Blank.getVal();
			String name = strs[0].trim();
			if (strs.length == 2) {
				instanceType = Integer.parseInt(strs[1].strip());
			}
			String filename = GlobalParam.INSTANCE_PATH + "/" + name + "/task.xml";
			InstanceConfig nconfig;
			if (this.instanceConfigs.containsKey(name)) {
				nconfig = this.instanceConfigs.get(name);
			} else {
				nconfig = new InstanceConfig(filename, instanceType);
				this.instanceConfigs.put(name, nconfig);
			}
			nconfig.init();
			if (nconfig.getAlias().equals("")) {
				nconfig.setAlias(name);
			}
			nconfig.setName(name);
			this.searchConfigMap.put(nconfig.getAlias(), nconfig);
		}
	}

	public Map<String, InstructionParam> getInstructions() {
		return this.instructions;
	}

	public Map<String, InstanceConfig> getInstanceConfigs() {
		return this.instanceConfigs;
	}

	public Map<String, InstanceConfig> getSearchConfigs() {
		return this.searchConfigMap;
	}

	public Map<String, WarehouseSqlParam> getSqlWarehouse() {
		return this.sqlWarehouse;
	}

	public Map<String, WarehouseNosqlParam> getNoSqlWarehouse() {
		return this.NoSqlWarehouse;
	}

	public void reload() {
		for (Map.Entry<String, InstanceConfig> e : this.instanceConfigs.entrySet()) {
			e.getValue().reload();
		}
	}

	public void addSource(RESOURCE_TYPE type, Object o) {
		switch (type) {
		case SQL:
			WarehouseSqlParam e1 = (WarehouseSqlParam) o;
			sqlWarehouse.put(e1.getAlias(), e1);
			break;

		case NOSQL:
			WarehouseNosqlParam e2 = (WarehouseNosqlParam) o;
			NoSqlWarehouse.put(e2.getAlias(), e2);
			break;

		case INSTRUCTION:
			InstructionParam e3 = (InstructionParam) o;
			instructions.put(e3.getId(), e3);
			break;
		}
	}

	private void parseInstructionsFile(String src) {
		InputStream in = null;
		try {
			byte[] bt = EFDataStorer.getData(src, false);
			if (bt.length <= 0)
				return;
			in = new ByteArrayInputStream(bt, 0, bt.length);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(in);

			Element sets = (Element) doc.getElementsByTagName("sets").item(0);
			NodeList paramlist = sets.getElementsByTagName("instruction");
			parseNode(paramlist, InstructionParam.class);

		} catch (Exception e) {
			Common.LOG.error("parse (" + src + ") error,", e);
		} finally {
			try {
				if (null != in) {
					in.close();
				}
			} catch (Exception e) {
				Common.LOG.error("parse (" + src + ") error,", e);
			}
		}
	}

	private void parsePondFile(String src) {
		InputStream in = null;
		try {
			byte[] bt = EFDataStorer.getData(src, false);
			if (bt.length <= 0)
				return;
			in = new ByteArrayInputStream(bt, 0, bt.length);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(in);
			NodeList paramlist;

			Element NoSql = (Element) doc.getElementsByTagName(RESOURCE_TYPE.NOSQL.name()).item(0);
			if(NoSql != null) {
				paramlist = NoSql.getElementsByTagName("socket");
				parseNode(paramlist, WarehouseNosqlParam.class);
			}

			Element Sql = (Element) doc.getElementsByTagName(RESOURCE_TYPE.SQL.name()).item(0);
			paramlist = Sql.getElementsByTagName("socket");
			parseNode(paramlist, WarehouseSqlParam.class);

		} catch (Exception e) {
			Common.LOG.error("parse (" + src + ") error,", e);
		} finally {
			try {
				if (null != in) {
					in.close();
				}
			} catch (Exception e) { 
				Common.LOG.error("parse (" + src + ") error,", e);
			}
		}
	}

	private void parseNode(NodeList paramlist, Class<?> c) throws Exception {
		if (paramlist != null && paramlist.getLength() > 0) {
			for (int i = 0; i < paramlist.getLength(); i++) {
				Node param = paramlist.item(i);
				if (param.getNodeType() == Node.ELEMENT_NODE) { 
					Object o = Common.getXmlObj(param, c);
					if (c == WarehouseNosqlParam.class) {
						addSource(RESOURCE_TYPE.NOSQL, o);
					} else if (c == WarehouseSqlParam.class) {
						addSource(RESOURCE_TYPE.SQL, o);
					} else if (c == InstructionParam.class) {
						addSource(RESOURCE_TYPE.INSTRUCTION, o);
					}
				}
			}
		}
	}
}
