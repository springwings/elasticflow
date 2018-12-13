package org.elasticflow.util.email;


public interface MailService {

	void sendMail() throws Exception;
	
	void sendMail(String subject,String content) throws Exception;
	
	void sendMailByAsynchronousMode() ;
	
	void sendMailByAsynchronousMode(String subject,String content) ; 
	
	void sendMailBySynchronizationMode()	throws Exception;
	
	void sendMailBySynchronizationMode(String subject,String content)	throws Exception;
}
