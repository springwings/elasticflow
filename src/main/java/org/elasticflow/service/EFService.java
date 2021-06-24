/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.service;

import java.util.HashMap;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:23
 */
public interface EFService {
	public void init(HashMap<String, Object> serviceParams);
	public void close();
	public void start();
}
