package org.elasticflow.util.email;

/** 
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */ 
public interface MailService {

	void sendMail() throws Exception;
	
	void sendMail(String subject,String content) throws Exception;
	
	void sendMailByAsynchronousMode() ;
	
	void sendMailByAsynchronousMode(String subject,String content) ; 
	
	void sendMailBySynchronizationMode()	throws Exception;
	
	void sendMailBySynchronizationMode(String subject,String content)	throws Exception;
}
