package org.elasticflow.param.pipe;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.Mechanism;

/**
 * data-flow trans parameters
 * @author chengwen
 * @version 4.0
 * @date 2018-10-25 16:14
 */
public class PipeParam { 
	private int readPageSize = GlobalParam.READ_PAGE_SIZE;
	private String writeTo;
	private String modelFrom;
	private String writeHandler;
	private boolean writerPoolShareAlias = true;
	private String searchFrom;
	private String searcherHandler;
	private boolean searcherShareAlias = true;
	private String readFrom;
	private String readHandler;
	private boolean readerPoolShareAlias = false;
	private String deltaCron;
	private String fullCron;
	private String optimizeCron; 
	private String instanceName;
	private String[] nextJob;
	/** default is slave pipe,if is master will only manage pipe with no detail transfer job! */
	private boolean isMaster = false;
	/**data write into type,full create new record,increment update part of data*/
	private String writeType="full";
	private Mechanism writeMechanism = Mechanism.AB;
	private boolean multiThread = false;
	
	 
	public String getWriteTo() {
		return writeTo;
	}
	public int getReadPageSize() {
		return readPageSize;
	}
	public String getModelFrom() {
		return modelFrom;
	}
	public Mechanism getWriteMechanism() {
		return writeMechanism;
	}
	public void setWriteTo(String writeTo) {
		this.writeTo = writeTo;
	}
	public String getReadFrom() {
		return readFrom;
	}
	public String[] getNextJob() {
		return nextJob;
	}
	public boolean isMultiThread() {
		return multiThread;
	} 
	public String getReadHandler() {
		return readHandler;
	}
	public String getWriteHandler() {
		return writeHandler;
	}
	public String getSearcherHandler() {
		return searcherHandler;
	} 
	public String getInstanceName() {
		return instanceName;
	} 
	public String getDeltaCron() {
		return deltaCron;
	}
	public void setDeltaCron(String deltaCron) {
		this.deltaCron = deltaCron;
	}
	public String getFullCron() {
		return fullCron;
	}
	public void setFullCron(String fullCron) {
		this.fullCron = fullCron;
	} 
	public String getOptimizeCron() {
		return optimizeCron;
	}
	public void setOptimizeCron(String optimizeCron) {
		this.optimizeCron = optimizeCron;
	}  
	
	public String getSearchFrom() {
		if(this.searchFrom==null){
			this.searchFrom = this.writeTo;
		}
		return this.searchFrom;
	} 
	
	public String getWriteType() {
		return writeType;
	} 
	
	public void setInstancename(String v) {
		this.instanceName = v;
	}
	
	public boolean isWriterPoolShareAlias() {
		return writerPoolShareAlias;
	} 
	
	public boolean isReaderPoolShareAlias() {
		return readerPoolShareAlias;
	} 

	public boolean isSearcherShareAlias() {
		return searcherShareAlias;
	}

	public boolean isMaster() {
		return isMaster;
	} 
	
	public static void setKeyValue(PipeParam PP,String k,String v){ 
		switch (k.toLowerCase()) {
		case "writeto":
			PP.writeTo = v;
			break;
		case "writerpoolsharealias":
			PP.writerPoolShareAlias = Boolean.valueOf(v);
			break;
		case "readerpoolsharealias":
			PP.readerPoolShareAlias = Boolean.valueOf(v);
			break;
		case "searchersharealias":
			PP.searcherShareAlias = Boolean.valueOf(v);
			break;
		case "readfrom":
			PP.readFrom = v;
			break;
		case "readpagesize":
			PP.readPageSize = Integer.valueOf(v);
			break;	
		case "modelfrom":
			PP.modelFrom = v;
			break;
		case "deltacron":
			PP.deltaCron = v;
			break;
		case "fullcron":
			PP.fullCron = v;
			break;
		case "optimizecron":
			PP.optimizeCron = v;
			break; 
		case "searchfrom":
			PP.searchFrom = v;
			break;
		case "searcherhandler":
			PP.searcherHandler = v;
			break;
		case "readhandler":
			PP.readHandler = v;
			break;
		case "writehandler":
			PP.writeHandler = v;
			break;
		case "instancename":
			PP.instanceName = v;
			break;
		case "nextjob":
			PP.nextJob = v.replace(",", " ").trim().split(" "); 
			break;
		case "ismaster":
			if(v.length()>0 && v.toLowerCase().equals("true"))
				PP.isMaster = true;
			break;
		case "writetype":
			if(v.length()>0 && (v.equals("full") || v.equals("increment")))
				PP.writeType = v;
			break;
		case "writemechanism":
			if(!v.toLowerCase().equals("ab")) {
				PP.writeMechanism = Mechanism.Time;
			}
			break;
		case "multithread":
			if(v.length()>0 && (v.equals("true")))
				PP.multiThread = true;
			break;
		}
	}
}
