package org.elasticflow.util;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:19
 */
public final class FNIoc {
	private static ApplicationContext ACT; 
	
	static {
		ACT = new ClassPathXmlApplicationContext ("spring.xml");
	} 

	public static Object getBean(String beanname) {
		return ACT.getBean(beanname);
	}
}
