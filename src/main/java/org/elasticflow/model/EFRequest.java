package org.elasticflow.model;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.param.end.SearcherParam;

/**
 * ElasticFlow Search Request parameters Model
 * @author chengwen
 * @version 1.0
 * @date 2018-10-22 09:08
 */
public class EFRequest{
	private String pipe = null;
	private String detail = null;
	private String originalKeyword = null;
	private Map<String, Object> params = new HashMap<String, Object>();
	private ArrayList<String> errors = new ArrayList<String>();

	public static EFRequest getInstance() {
		return new EFRequest();
	}

	public boolean hasErrors() {
		if (errors.size() > 0)
			return true;
		return false;
	}

	public String getErrors() {
		String err = "";
		for (String s : errors) {
			err += s + ",";
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
		if (key != null && key.length() > 0 && value != null && String.valueOf(value).length() > 0) {
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
		if(this.params.containsKey(key))
			return this.params.get(key);
		return null;
	}
	
	public int getIntParam(String key) {
		return Integer.valueOf(getStringParam(key));
	}
	
	public String getStringParam(String key) {
		if(this.params.containsKey(key))
			return String.valueOf(this.params.get(key));
		return ""; 
	}
	
	public boolean getBooleanParam(String key) {
		if(this.params.containsKey(key))
			return Boolean.valueOf(String.valueOf(this.params.get(key)));
		return false; 
	}

	public Map<String, Object> getParams() {
		return this.params;
	}

	public Object get(String key, SearcherParam sp, String type) { 
		Object v = null;
		if (params.containsKey(key)) {
			v = params.get(key);
		} else {
			if(sp != null)
				v = sp.getDefaultValue();
		}
		if(v==null)
			return null;
		try {
			Class<?> c = Class.forName(type);
			Method method = c.getMethod("valueOf", String.class);
			return method.invoke(c, String.valueOf(v));
		} catch (Exception e) {
			addError("param " + key + " parse Exception!");
			e.printStackTrace();
		}
		return null;
	}

	public void addError(String e) {
		this.errors.add(e);
	}
}