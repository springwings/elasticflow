/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.model.searcher;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-22 09:08
 */
public class ResponseDataUnit {
	
	Map<String,Object> internalMap = new LinkedHashMap<String, Object>();
	
	public static ResponseDataUnit getInstance(){
		return new ResponseDataUnit();
	}
	
	public void addObject(String key, Object o){
		internalMap.put(key, o);
	}
	
	public Map<String , Object> getContent(){
		return internalMap;
	}
}
