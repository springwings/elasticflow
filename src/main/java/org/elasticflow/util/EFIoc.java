/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.util;

import java.io.File;

import org.apache.log4j.PropertyConfigurator;
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
		String logpath = (String) Common.loadProperties(GlobalParam.SYS_CONFIG_PATH + "/log4j.properties").get("log4j.appender.EF.file");
		PropertyConfigurator.configure(GlobalParam.SYS_CONFIG_PATH + "/log4j.properties");
		File test_write = new File(logpath);
		if(test_write.canWrite()) {
			ACT = new ClassPathXmlApplicationContext("spring.xml");
		} else {
			System.out.println(logpath+" does not have write permission!");
			System.exit(0);
		}
	} 
	public static Object getBean(String beanname) {
		return ACT.getBean(beanname);
	}
}
