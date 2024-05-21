/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.yarn.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Pipe XML configure file operation
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:21
 */
public class PipeXMLUtil {

	/**
	 * modify xml file node
	 * @param xmlPath
	 * @param taglevel    example: TransParam.param
	 * @param tagName     example: remark
	 * @param tagValue
	 * @throws EFException
	 */
	public static void ModifyNode(String xmlPath, String taglevel, String tagName, String tagValue) throws EFException {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		try (InputStream is = new FileInputStream(xmlPath)) {
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(is);
			String[] tags = taglevel.split("\\.");

			if (tags.length != 2)
				throw new EFException("tag Must be within two levels.");

			Element dataflow = (Element) doc.getElementsByTagName("dataflow").item(0);
			Element nodes = (Element) dataflow.getElementsByTagName(tags[0]).item(0);
			NodeList paramlist = nodes.getElementsByTagName(tags[1]);
			if (paramlist != null && paramlist.getLength() > 0) {
				for (int i = 0; i < paramlist.getLength(); i++) {
					Element element = (Element) paramlist.item(i);
					if (element.getElementsByTagName("name").item(0).getTextContent().equals(tagName)) {
						Element ele = (Element) element.getElementsByTagName("value").item(0);
						ele.setTextContent(tagValue);
						break;
					}
				}
			}
			writeXml(doc, xmlPath);
		} catch (Exception e) {
			throw new EFException(e);
		}
	}

	private static void writeXml(Document doc, String outPath) throws Exception {
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		DOMSource source = new DOMSource(doc);
		try (FileOutputStream output = new FileOutputStream(outPath)) {
			StreamResult result = new StreamResult(output);
			transformer.transform(source, result);
		}
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(outPath), "UTF-8"))) {
			String s = null;
			StringBuilder result = new StringBuilder();
			while ((s = br.readLine()) != null) {
				if (s.strip().length() > 1)
					result.append(s + System.lineSeparator());
			}
			try (BufferedWriter bw = new BufferedWriter(new FileWriter(outPath))) {
				bw.write(result.toString().strip());
			}
		} catch (Exception e) {
			throw new EFException(e);
		}
	}
	
	/**
	 * scan all modules
	 * @param rq
	 * @param RR
	 */
	public static JSONArray getModules(){
		JSONArray res = new JSONArray();
		ArrayList<File> modules = EFFileUtil.scanFolders(GlobalParam.INSTANCE_PATH);
		Map<String, InstanceConfig> configs = Resource.nodeConfig.getInstanceConfigs();
		for(File f : modules) {
			JSONObject row = new JSONObject();
			row.put("name", f.getName());
			row.put("path", f.getPath());
			row.put("enable", configs.containsKey(f.getName()));
			row.put("opencompute",false);
			row.put("readtype",new String[]{});
			row.put("writetype",new String[]{});
			row.put("computetype","");
			byte[] bt = EFDataStorer.getData(f.getPath()+"/task.xml", false);  
			try {
				DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
				DocumentBuilder db = dbf.newDocumentBuilder();
				Document doc = db.parse(new ByteArrayInputStream(bt, 0, bt.length));
				Element dataflow = (Element) doc.getElementsByTagName("dataflow").item(0); 
				if (dataflow.getElementsByTagName("ComputerParam").item(0) != null) 
					row.put("opencompute",true);  
				Element TransParam = (Element) dataflow.getElementsByTagName("TransParam").item(0);  
				row.put("desc",getXmlNodeInfo(TransParam.getElementsByTagName("param"),"remark","value"));  
				row.put("readfrom",getXmlNodeInfo(TransParam.getElementsByTagName("param"),"readFrom","value")); 
				row.put("writeto",getXmlNodeInfo(TransParam.getElementsByTagName("param"),"writeTo","value")); 
				Element ComputerParam = (Element) dataflow.getElementsByTagName("ComputerParam").item(0);  
				if(ComputerParam!=null) {
					row.put("computeurl",getXmlNodeInfo(ComputerParam.getElementsByTagName("param"),"api","value"));
					row.put("computetype",getXmlNodeInfo(ComputerParam.getElementsByTagName("param"),"computeMode","value"));
				}else {
					row.put("computetype","");
				}	
				row.put("readtype",getXmlNodeInfo(TransParam.getElementsByTagName("param"),"readFrom","type").split(","));  
				row.put("writetype",getXmlNodeInfo(TransParam.getElementsByTagName("param"),"writeTo","type").split(","));   
			} catch (Exception e) {  
				Common.LOG.warn("instance {} exception {}",f.getName(),e.getMessage());
			}			
			res.add(row);
		}
		return res;
	}
	
	private static String getXmlNodeInfo(NodeList paramlist,String name,String key) throws EFException { 
		try {
			for (int i = 0; i < paramlist.getLength(); i++) {
				Node param = paramlist.item(i); 
				if(param.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) param; 
					if(element.getElementsByTagName("name").item(0).getTextContent().equals(name))  
						return element.getElementsByTagName(key).item(0).getTextContent();
				}
			}
		} catch (Exception e) {
			throw new EFException(e,"get xml tag "+name+" params "+key+" value exception.",ELEVEL.Dispose); 
		}
		return "";
	}
}
