package org.elasticflow.config;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.elasticflow.config.GlobalParam.INSTANCE_TYPE;
import org.elasticflow.field.EFField;
import org.elasticflow.param.BasicParam;
import org.elasticflow.param.end.ReaderParam;
import org.elasticflow.param.end.SearcherParam;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.ml.ComputeParam;
import org.elasticflow.param.pipe.PipeParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.ConfigStorer;
import org.elasticflow.yarn.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * instance Configuration model
 * @author chengwen
 * @version 3.1 
 * @date 2018-10-11 15:13
 */
public class InstanceConfig {

	private String filename;
	private String alias = "";
	private boolean status = true;
	private String name;
	private volatile Map<String, EFField> writeFields;
	private volatile Map<String, String> externConfigs;
	private volatile Map<String, EFField> computeFields;
	private volatile Map<String,SearcherParam> searcherParams;
	private volatile WriterParam writerParams;
	private volatile PipeParam pipeParams;
	private volatile ReaderParam readParams;
	private volatile ComputeParam computeParams;
	private int instanceType = INSTANCE_TYPE.Blank.getVal();  

	public InstanceConfig(String fileName, int instanceType) {
		this.filename = fileName; 
		this.instanceType = instanceType;
	}

	public void init() { 
		this.pipeParams = new PipeParam();
		this.writeFields = new HashMap<>();
		this.computeFields = new HashMap<>();
		this.searcherParams = new HashMap<>();
		this.externConfigs = new HashMap<>();
		this.computeParams = new ComputeParam();
		this.writerParams = new WriterParam();
		loadInstanceConfig();
		Common.LOG.info(filename + " config loaded");
	}

	public void reload() {
		Common.LOG.info("starting reload " + filename);
		init();
	} 
	
	public boolean checkWriteField(String key, String value) {
		if (!writeFields.containsKey(key)) {
			return true;
		} else {
			return writeFields.get(key).isValid(value);
		}
	}

	public EFField getWriteField(String key) {
		return writeFields.get(key);
	} 
	
	public SearcherParam getSearcherParam(String key) {
		return searcherParams.get(key);
	}
	
	public ComputeParam getComputeParams() {
		return computeParams;
	}
	
	public WriterParam getWriterParams() {
		return writerParams;
	}
	
	public ReaderParam getReadParams() {
		return readParams;
	}
	
	public PipeParam getPipeParams() {
		return pipeParams;
	}
	 
	public Map<String, String> getExternConfigs() {
		return externConfigs;
	} 
	
	public Map<String, EFField> getWriteFields() {
		return writeFields;
	}
	
	public Map<String, EFField> getComputeFields() {
		return computeFields;
	}
 
	public boolean openTrans() {
		if((instanceType & INSTANCE_TYPE.Trans.getVal()) > 0){
			if(pipeParams.getReadFrom()!=null && pipeParams.getWriteTo()!=null){
				return true;
			}
		}
		return false;
	}
	
	public boolean openCompute() {
		if((instanceType & INSTANCE_TYPE.WithCompute.getVal()) > 0) {
			return true;
		}
		return false;
	}
 
	public int getInstanceType(){
		return this.instanceType;
	} 

 
	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getAlias() {
		return this.alias;
	} 
	
	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return this.name;
	} 
 
	public boolean checkStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	private void loadInstanceConfig() {
		InputStream in;
		try {
			byte[] bt = ConfigStorer.getData(this.filename,false);
			if (bt.length <= 0)
				return;
			in = new ByteArrayInputStream(bt, 0, bt.length);
			configParse(in);
			in.close();
		} catch (Exception e) {
			in = null;
			setStatus(false);
			Common.LOG.error("load Instance Config error,",e);
		}
	}
	
	/**
	 * node xml config parse
	 * searchparams store in readParamTypes all can for search
	 */
	private void configParse(InputStream in) {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(in);
		 
			Element params;
			NodeList paramlist; 
			 
			Element dataflow = (Element) doc.getElementsByTagName("dataflow").item(0);

			if (dataflow!=null) {  
				if (!dataflow.getAttribute("alias").equals("")) {
					this.alias = dataflow.getAttribute("alias");
				}  
				
				params = (Element) dataflow.getElementsByTagName("TransParam").item(0); 
				if (params!=null) {
					parseNode(params.getElementsByTagName("param"), "pipeParam", PipeParam.class);
				}else{
					Common.LOG.error(this.filename+" config setting not correct");
					return;
				}
				
				params = (Element) dataflow.getElementsByTagName("ReadParam").item(0);
				if(params!=null) {
					readParams = new ReaderParam();
					if(Resource.nodeConfig.getSqlWarehouse().containsKey(pipeParams.getReadFrom())) { 
						readParams.setNoSql(false);
						parseNode(params.getElementsByTagName("param"), "readParam",
								ReaderParam.class);  
					}else { 
						readParams.setNoSql(true);
						parseNode(params.getElementsByTagName("param"), "readParam",
								ReaderParam.class);
					}  
				} 
				
				params = (Element) dataflow.getElementsByTagName("ExternConfig").item(0);
				if(params!=null) { 
					parseNode(params.getElementsByTagName("param"), "common",
							String.class);
				} 
				
				params = (Element) dataflow.getElementsByTagName("ComputeParam").item(0);   
				if (params!=null) {
					parseNode(params.getElementsByTagName("param"), "computeParam", ComputeParam.class);
					params = (Element) params.getElementsByTagName("fields").item(0);
					paramlist = params.getElementsByTagName("field");
					parseNode(paramlist, "computeFields", EFField.class); 
				}
				
				params = (Element) dataflow.getElementsByTagName("WriteParam").item(0);   
				if (params!=null) { 
					parseNode(params.getElementsByTagName("param"), "writerParam", BasicParam.class);
					if(writerParams.getWriteKey()==null) {
						WriterParam.setKeyValue(writerParams,"writekey",readParams.getKeyField());
						WriterParam.setKeyValue(writerParams,"keytype","unique"); 
					}
					params = (Element) params.getElementsByTagName("fields").item(0);
					paramlist = params.getElementsByTagName("field");
					parseNode(paramlist, "writeFields", EFField.class); 
				}
				
				params = (Element) dataflow.getElementsByTagName("SearchParam").item(0);   
				if (params!=null) {
					paramlist = params.getElementsByTagName("param");
					parseNode(paramlist, "SearchParam", SearcherParam.class);
				} 
				
			} 
			
		} catch (Exception e) {
			setStatus(false);
			Common.LOG.error(this.filename+" configParse error,",e);
		}
	}
	
	private void parseNode(NodeList paramlist, String type, Class<?> c)
			throws Exception {
		if (paramlist != null && paramlist.getLength() > 0) {
			for (int i = 0; i < paramlist.getLength(); i++) {
				Node param = paramlist.item(i);
				if (param.getNodeType() == Node.ELEMENT_NODE) { 
					switch (type) {
					case "writeFields":
						EFField wf = (EFField) Common.getXmlObj(param, c);
						writeFields.put(wf.getName(), wf);
						break;
					case "computeFields":
						EFField cf = (EFField) Common.getXmlObj(param, c);
						computeFields.put(cf.getName(), cf);
						break;
					case "computeParam":
						Common.getXmlParam(computeParams, param, c); 
						break;
					case "writerParam":
						BasicParam wpp = (BasicParam) Common.getXmlObj(param, c); 
						WriterParam.setKeyValue(writerParams,wpp.getName(), wpp.getValue());
						break;
					case "pipeParam":
						Common.getXmlParam(pipeParams, param, c);  
						break; 
					case "common": 
						Common.getXmlParam(externConfigs, param, c);
						break; 
					case "readParam": 
						Common.getXmlParam(readParams, param, c);
						break; 
					case "SearchParam":
						SearcherParam v  = (SearcherParam) Common.getXmlObj(param, c);
						searcherParams.put(v.getName(), v);
						break;   
					}
				}
			}
		}
	} 
}
