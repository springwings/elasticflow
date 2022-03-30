package org.elasticflow.notifier;

/**
 * Notifier interface
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-31 10:52
 * @modify 2021-06-11 10:45
 */
public interface EFNotify {
	
	/**
	 * Notifier
	 * @param subject    information subject
	 * @param instance   
	 * @param content    information contents
	 * @param errorType  
	 * @param sync       is synchronous send mode or not
	 * @return
	 */
	public boolean send(final String subject,final String instance, final String content, final String errorType,boolean sync);
}
