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

import org.elasticflow.config.GlobalParam.INSTANCE_TYPE;
import org.elasticflow.config.GlobalParam.RESOURCE_TYPE;
import org.elasticflow.param.pipe.InstructionParam;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.util.EFNodeUtil;
import org.elasticflow.util.instance.EFDataStorer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * The EF node configuration control center,
 * manages the abstract flow and data source configuration.
 * 
 * @author chengwen
 * @version 4.1
 * @date 2018-10-11 14:50
 */
public class NodeConfig {
	
	/**instance,instance-config map*/
	private final Map<String, InstanceConfig> instanceConfigs = new HashMap<>();
	private final Map<String, InstanceConfig> searchConfigMap = new HashMap<>();
	private final Map<String, WarehouseParam> warehouse = new HashMap<>();
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

	public void init(String instanceSettings) {
		loadConfig(instanceSettings, true); 
	}

	public void loadConfig(String instanceSettings, boolean reset) {
		if (reset) {
			this.instanceConfigs.clear();
			this.searchConfigMap.clear();
			parsePondFile(GlobalParam.CONFIG_PATH + "/" + this.pondFile);
			parseInstructionsFile(GlobalParam.CONFIG_PATH + "/" + this.instructionsFile);
		} 
		if(EFNodeUtil.isMaster())
			loadInstanceConfig(instanceSettings);
	}
	
	public void loadInstanceConfig(String instanceSettings) {  
		if (instanceSettings.trim().length() < 1)
			return;
		for (String inst : instanceSettings.split(",")) {
			String[] strs = inst.split(":");
			if (strs.length < 1)
				continue;
			int instanceType = INSTANCE_TYPE.Blank.getVal();
			String name = strs[0].trim();
			if (strs.length == 2) {
				instanceType = Integer.parseInt(strs[1].trim());
			} 
			InstanceConfig nconfig;
			if (this.instanceConfigs.containsKey(name)) {
				nconfig = this.instanceConfigs.get(name);
			} else {
				nconfig = new InstanceConfig(EFFileUtil.getInstancePath(name)[1], instanceType);
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

	public Map<String, WarehouseParam> getWarehouse() {
		return this.warehouse;
	}


	public void reload() {
		for (Map.Entry<String, InstanceConfig> e : this.instanceConfigs.entrySet()) {
			e.getValue().reload();
		}
	}

	public void addSource(RESOURCE_TYPE type, Object o) {
		switch (type) {
		case WAREHOUSE:
			WarehouseParam e1 = (WarehouseParam) o;
			warehouse.put(e1.getAlias(), e1);
			break;

		case INSTRUCTION:
			InstructionParam e3 = (InstructionParam) o;
			instructions.put(e3.getId(), e3);
			break;
		}
	}

	public void parseInstructionsFile(String src) {
		InputStream in = null;
		instructions.clear();
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

	public void parsePondFile(String src) {
		InputStream in = null;
		warehouse.clear();
		try {
			byte[] bt = EFDataStorer.getData(src, false);
			if (bt.length <= 0)
				return;
			in = new ByteArrayInputStream(bt, 0, bt.length);
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(in);
			NodeList paramlist;

			paramlist = doc.getElementsByTagName("socket");
			parseNode(paramlist, WarehouseParam.class);
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
					if (c == WarehouseParam.class) {
						addSource(RESOURCE_TYPE.WAREHOUSE, o);					
					} else if (c == InstructionParam.class) {
						addSource(RESOURCE_TYPE.INSTRUCTION, o);
					}
				}
			}
		}
	}
}
