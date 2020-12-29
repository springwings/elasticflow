package org.elasticflow.util.email;

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
 
/** 
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */ 
public class EFAuthenticator extends Authenticator{
	String userName=null;
	String password=null;
	 
	public EFAuthenticator(){
	}
	public EFAuthenticator(String username, String password) { 
		this.userName = username; 
		this.password = password; 
	} 
	protected PasswordAuthentication getPasswordAuthentication(){
		return new PasswordAuthentication(userName, password);
	}
}
 
