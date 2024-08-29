package org.elasticflow.config;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.elasticflow.config.GlobalParam.INSTANCE_TYPE;
import org.elasticflow.field.EFField;
import org.elasticflow.param.BasicParam;
import org.elasticflow.param.end.ComputerParam;
import org.elasticflow.param.end.ReaderParam;
import org.elasticflow.param.end.SearcherParam;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.PipeParam;
import org.elasticflow.util.Common;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.yarn.Resource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * instance Configuration model
 * -Fields is Metadata describing data 
 * -Params is Parameter of control end
 * 
 * @author chengwen
 * @version 5.1
 * @date 2018-10-11 15:13
 */
public class InstanceConfig {

	private String alias = "";
	/** Instance Config load success **/
	private boolean status = true;
	private String configPath;
	/** Use the configured file name **/
	private String instanceID;
	
	/**reader Configured Field Information*/
	private volatile Map<String, EFField> readFields;
	/**writer Configured Field Information*/
	private volatile Map<String, EFField> writeFields;
	/**computer Configured Field Information*/
	private volatile Map<String, EFField> computeFields;
	/**searcher Configured Field Information*/
	private volatile Map<String, EFField> searchFields;
	/**searcher Configured parameters Information,Note that this will be a StrongReference be-careful*/
	private volatile Map<String, SearcherParam> searcherParams;
	/**writer Configured parameters Information*/
	private volatile WriterParam writerParams;
	/**piper Configured parameters Information*/
	private volatile PipeParam pipeParams;
	/**reader Configured parameters Information*/
	private volatile ReaderParam readerParams;
	/**computer Configured parameters Information*/
	private volatile ComputerParam computerParams;
	
	private int instanceType = INSTANCE_TYPE.Blank.getVal();
	private boolean hasFullJob = true;

	public InstanceConfig(String configPath, int instanceType) {
		this.configPath = configPath;
		this.instanceType = instanceType;
	}

	public void init() {
		this.searchFields = new HashMap<>();
		this.writeFields = new HashMap<>();
		this.computeFields = new HashMap<>();
		this.readFields = new HashMap<>();
		this.pipeParams = new PipeParam();
		this.searcherParams = new HashMap<>();
		this.computerParams = new ComputerParam();
		this.writerParams = new WriterParam();
		loadInstanceConfig(); 
		if(searchFields.size()==0) {
			for (Entry<String, EFField> entry : writeFields.entrySet()) { 
				EFField val = entry.getValue();
				searchFields.put(val.getAlias(), val);
			}
		}
	}

	/**
	 * Reload all information in the configuration instance from the configuration file
	 */
	public void reload() {
		Common.LOG.info("start to reload from {}",configPath);
		init();
	}

	public EFField getWriteField(String key) {
		return this.writeFields.get(key);
	}

	public SearcherParam getSearcherParam(String key) {
		return this.searcherParams.get(key);
	}

	public ComputerParam getComputeParams() {
		return this.computerParams;
	}

	public WriterParam getWriterParams() {
		return this.writerParams;
	}

	public ReaderParam getReaderParams() {
		return this.readerParams;
	}

	public PipeParam getPipeParams() {
		return this.pipeParams;
	}

	public Map<String, EFField> getWriteFields() {
		return this.writeFields;
	}
	
	/**
	 * Note that this will be a StrongReference be-careful,
	 * it is Reference to writeFields
	 * @return
	 */
	public Map<String, EFField> getSearchFields(){
		return this.searchFields;
	}

	public Map<String, EFField> getComputeFields() {
		return this.computeFields;
	}

	public Map<String, EFField> getReadFields() {
		return this.readFields;
	}

	public boolean getHasFullJob() {
		return this.hasFullJob;
	}

	public boolean setHasFullJob(boolean hasFullJob) {
		return this.hasFullJob = hasFullJob;
	}

	public boolean openTrans() {
		if ((this.instanceType & INSTANCE_TYPE.Trans.getVal()) > 0) {
			if (this.pipeParams.getReadFrom() != null && this.pipeParams.getWriteTo() != null) {
				return true;
			}
		}
		return false;
	}

	public boolean openCompute() {
		if ((this.instanceType & INSTANCE_TYPE.WithCompute.getVal()) > 0) {
			return true;
		}
		return false;
	}

	public int getInstanceType() {
		return this.instanceType;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getAlias() {
		return this.alias;
	}

	public void setInstanceID(String instanceID) {
		this.instanceID = instanceID;
	}

	public String getInstanceID() {
		return this.instanceID;
	}

	public boolean checkStatus() {
		return this.status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	private void loadInstanceConfig() {
		InputStream in;
		try {
			byte[] bt = EFDataStorer.getData(this.configPath, false);
			if (bt.length <= 0) {
				setStatus(false);
				Common.systemLog("load instance failed, task configuration file {} not exists!", this.configPath);
				return;
			}				
			in = new ByteArrayInputStream(bt, 0, bt.length);
			configParse(in);
			in.close();
			Common.LOG.info("load instance configuration from {} success!",this.configPath);
		} catch (Exception e) {
			in = null;
			setStatus(false);
			Common.systemLog("load instance configuration from {} exception",this.configPath, e);
		}
	}

	/**
	 * node xml config parse searchparams store in readParamTypes all can for search
	 */
	private void configParse(InputStream in) throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(in);

		Element params;
		NodeList paramlist;

		Element dataflow = (Element) doc.getElementsByTagName("dataflow").item(0);

		if (dataflow != null) {
			if (!dataflow.getAttribute("alias").equals("")) {
				this.alias = dataflow.getAttribute("alias");
			}
			params = (Element) dataflow.getElementsByTagName("TransParam").item(0);
			if (params != null) {
				parseNode(params.getElementsByTagName("param"), "pipeParam", PipeParam.class);
			} else {
				Common.systemLog("{} TransParam not set",this.configPath);
				return;
			}

			params = (Element) dataflow.getElementsByTagName("ReaderParam").item(0);
			if (params != null) {
				readerParams = new ReaderParam();
				if (Resource.nodeConfig.getWarehouse().containsKey(pipeParams.getReadFrom())) {
					readerParams.setNoSql(false);
					parseNode(params.getElementsByTagName("param"), "readerParam", ReaderParam.class);
				} else {
					readerParams.setNoSql(true);
					parseNode(params.getElementsByTagName("param"), "readerParam", ReaderParam.class);
				}
				params = (Element) params.getElementsByTagName("fields").item(0);
				if (params != null) {
					paramlist = params.getElementsByTagName("field");
					parseNode(paramlist, "readFields", EFField.class);
				}
			}

			params = (Element) dataflow.getElementsByTagName("ComputerParam").item(0);
			if (params != null) {
				parseNode(params.getElementsByTagName("param"), "computerParam", ComputerParam.class);
				params = (Element) params.getElementsByTagName("fields").item(0);
				if (params != null) {
					paramlist = params.getElementsByTagName("field");
					parseNode(paramlist, "computeFields", EFField.class);
				}
			}

			params = (Element) dataflow.getElementsByTagName("WriterParam").item(0);
			if (params != null) {
				parseNode(params.getElementsByTagName("param"), "writerParam", BasicParam.class);
				if (writerParams.getWriteKey() == null) {
					WriterParam.setKeyValue(writerParams, "writekey", readerParams.getKeyField());
					WriterParam.setKeyValue(writerParams, "keytype", "unique");
				}
				params = (Element) params.getElementsByTagName("fields").item(0);
				if (params != null) {
					paramlist = params.getElementsByTagName("field");
					parseNode(paramlist, "writeFields", EFField.class);
				}
			}

			params = (Element) dataflow.getElementsByTagName("SearcherParam").item(0);
			if (params != null) {
				paramlist = params.getElementsByTagName("param");
				parseNode(paramlist, "SearchParam", SearcherParam.class);
			}

		}
	}
	
	private void addField(String name,EFField field,Map<String, EFField> container) {
		if(container.containsKey(name)) {
			Common.LOG.warn("{} duplicate field {} definitions",this.instanceID,name);
		}else {
			container.put(name, field);
		}
	}
	
	private void parseNode(NodeList paramlist, String type, Class<?> c) throws Exception {
		if (paramlist != null && paramlist.getLength() > 0) {
			for (int i = 0; i < paramlist.getLength(); i++) {
				Node param = paramlist.item(i);
				if (param.getNodeType() == Node.ELEMENT_NODE) {
					switch (type) {
					case "writeFields":
						EFField wf = (EFField) Common.getXmlObj(param, c);
						addField(wf.getName(),wf,writeFields);
						break;
					case "searchFields":
						EFField sf = (EFField) Common.getXmlObj(param, c);
						searchFields.put(sf.getName(), sf);
						break;
					case "readFields":
						EFField rf = (EFField) Common.getXmlObj(param, c);
						readFields.put(rf.getName(), rf);
						break;
					case "computeFields":
						EFField cf = (EFField) Common.getXmlObj(param, c); 
						addField(cf.getName(),cf,computeFields);
						break;
					case "computerParam":
						Common.getXmlParam(computerParams, param, c);
						break;
					case "writerParam":
						BasicParam wpp = (BasicParam) Common.getXmlObj(param, c);
						WriterParam.setKeyValue(writerParams, wpp.getName(), wpp.getValue());
						break;
					case "pipeParam":
						Common.getXmlParam(pipeParams, param, c);
						pipeParams.reInit();
						break;
					case "readerParam":
						Common.getXmlParam(readerParams, param, c);
						break;
					case "SearcherParam":
						SearcherParam v = (SearcherParam) Common.getXmlObj(param, c);
						searcherParams.put(v.getName(), v);
						break;
					}
				}
			}
		}
	}
}