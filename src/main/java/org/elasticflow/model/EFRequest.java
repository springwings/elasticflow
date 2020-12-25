package org.elasticflow.model;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.param.end.SearcherParam;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-22 09:08
 */
public class EFRequest {
	private String pipe = null;
	private String detail = null;
	private String originalKeyword = null; 
	private Map<String, Object> params = new HashMap<String, Object>(); 
	private ArrayList<String> errors = new ArrayList<String>(); 

	public static EFRequest getInstance() {
		return new EFRequest();
	} 
	
	public boolean hasErrors() {
		if(errors.size()>0)
			return true;
		return false;
	}
	
	public String getErrors(){
		String err="";
		for(String s:errors){
			err+=s+",";
		}
		return err;
	}
 
	public String getPipe() {
		return this.pipe;
	}

	public void setPipe(String pipe) {
		this.pipe = pipe;
	}

	public String getDetail() {
		return this.detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

	public String toString() {
		return this.pipe + ":" + params.toString();
	}

	public boolean isValid() {
		return this.pipe != null && this.pipe.length() > 0;
	}

	public boolean addParam(String key, Object value) {
		if (key != null && key.length() > 0 && value != null && String.valueOf(value).length() > 0){
			params.put(key, value);
			if (key.equals(GlobalParam.PARAM_KEYWORD))
				this.originalKeyword = String.valueOf(value);
		} 
		return true;
	}

	public String getOriginalKeyword() {
		return this.originalKeyword;
	}

	public void setOriginalKeyword(String originalKeyword) {
		this.originalKeyword = originalKeyword;
	}

	public Object getParam(String key) {
		return this.params.get(key);
	}

	public Map<String, Object> getParams() {
		return this.params;
	}  

	public Object get(String key, SearcherParam sp,String type) {
		if (sp == null)
			return null;
		Object v;
		if (params.containsKey(key)) {
			v = params.get(key);
		}else{
			v = sp.getDefaultValue();
		} 
		try {
			Class<?> c = Class.forName(type);
			Method method = c.getMethod("valueOf", String.class);
			return method.invoke(c,String.valueOf(v)); 
		} catch (Exception e) {
			addError("param "+key+" parse Exception!");
			e.printStackTrace();
		}
		return null;
	} 
	
	public void addError(String e){
		this.errors.add(e); 
	} 
}