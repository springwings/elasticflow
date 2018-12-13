package org.elasticflow.param.pipe;

import java.util.ArrayList;

import org.elasticflow.model.InstructionTree;
import org.elasticflow.util.Common;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
public class InstructionParam {
	
	private String id;
	private String cron;
	private ArrayList<InstructionTree> code= new ArrayList<>();
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getCron() {
		return cron;
	}
	public void setCron(String cron) {
		this.cron = cron;
	} 
	public ArrayList<InstructionTree> getCode() {
		return this.code;
	}
	public void setCode(String code) {
		 this.code = Common.compileCodes(code, this.id);
	}
}
