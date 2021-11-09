/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.field;

import org.elasticflow.util.EFException;

/**
 * must implementation 
 * public static T1 valueOf(Object data) throws EFException;
 * public static T2 getInstance(Object data) throws EFException;
 * @author chengwen
 * @version 1.0
 * @date 2018-11-20 10:47
 */
public interface FieldHandler<EF_VALUE_TYPE>{ 
	
	/**Domain value fetch function**/
	public String toString();
	
	public boolean isValid();
	
	public EF_VALUE_TYPE parse(Object s) throws EFException;
	
}
