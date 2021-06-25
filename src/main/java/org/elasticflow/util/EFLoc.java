/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.util;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:19
 */
public final class EFLoc {
	private static ApplicationContext ACT; 
	
	static {
		ACT = new ClassPathXmlApplicationContext ("spring.xml");
	} 

	public static Object getBean(String beanname) {
		return ACT.getBean(beanname);
	}
}
