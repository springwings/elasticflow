/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.param.end;

import com.alibaba.fastjson.JSONObject;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-09 11:36
 */
public class WriterParam {
	
	private String writeKey;
	/**keyType:scan|unique
		scan,scan update batch record
		unique key update single record
	*/
	private String keyType;
	/**user define field,pass custom value**/
	private JSONObject customParams;
	/** dsl parse method  normal/condition**/
	private String dslParse = "normal";
	
	private String handler;

	public String getWriteKey() {
		return writeKey;
	}

	public String getKeyType() {
		return keyType;
	}
	
	public JSONObject getCustomParams() {
		return customParams;
	}

	public String getDslParse() {
		return dslParse;
	}
	
	public String getHandler() {
		return handler;
	} 

	public static void setKeyValue(WriterParam wp, String k, String v) {
		switch (k.toLowerCase()) {
		case "writekey":
			wp.writeKey = v;
			break;
		case "keytype":
			wp.keyType = v;
			break;
		case "customParams":
			if(v!=null) {
				wp.customParams = JSONObject.parseObject(v);
			}	
			break;
		case "dslparse":
			wp.dslParse = v;
			break;
		case "handler":
			wp.handler = v;
			break;
		}
	}
}
