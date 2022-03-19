/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.flow;

/**
 * Socket interface
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public interface Socket<T> {

	T getSocket(Object... args);

}
