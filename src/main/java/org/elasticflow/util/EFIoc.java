/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.util;

import java.io.FileNotFoundException;

import org.elasticflow.config.GlobalParam;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * spring inversion of control
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:19
 */
public final class EFIoc {

	private static ApplicationContext ACT;

	static {
		try {
			org.springframework.util.Log4jConfigurer.initLogging(GlobalParam.configPath + "/log4j.properties");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		ACT = new ClassPathXmlApplicationContext("spring.xml");
	}

	public static Object getBean(String beanname) {
		return ACT.getBean(beanname);
	}
}
