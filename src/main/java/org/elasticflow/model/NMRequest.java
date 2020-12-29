package org.elasticflow.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
* ElasticFlow Node monitoring accepted parameters
* @author chengwen
* @version 1.0
* @data 2020-12-24
*/

public class NMRequest {
	
	private Map<String, String> params = new HashMap<String, String>(); 
	private ArrayList<String> errors = new ArrayList<String>(); 
	
	public static NMRequest getInstance() {
		return new NMRequest();
	} 
	
	public boolean addParam(String key, String value) {
		if (key != null && key.length() > 0 && value != null && value.length() > 0){
			params.put(key, value);
		} 
		return true;
	}
	
	public String getParam(String key) {
		return this.params.get(key);
	}

	public Map<String, String> getParams() {
		return this.params;
	}  
	
	public void addError(String e){
		this.errors.add(e); 
	} 
}
