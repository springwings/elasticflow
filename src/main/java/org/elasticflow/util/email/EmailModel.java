package org.elasticflow.util.email;

import java.io.Serializable;

import org.springframework.util.StringUtils;

/** 
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */ 
public class EmailModel implements Serializable{   
	private static final long serialVersionUID = -6208279516744390800L;
	private String from; 
	private String address;  
	private String bcc;  
	private String cc; 
	private String subject; 
	private String content; 
	public EmailModel(){ 
	}
	
	public EmailModel(String from,String address,String bcc,String cc,String subject,String content){
		this.from=from;
		this.address=address;
		this.bcc=bcc;
		this.cc=cc;
		this.subject=subject;
		this.content=content;
	}
	
	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}
	
	public void setAddress(String address) {
		this.address = address;
	}

	public String getBcc() {
		return bcc;
	}

	public void setBcc(String bcc) {
		this.bcc = bcc;
	}

	public String getCc() {
		return cc;
	}

	public void setCc(String cc) {
		this.cc = cc;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
	
	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}
	
	public String[] getAddress() {
		if (!StringUtils.hasLength(this.address)) {
			return null;
		}
		address = address.trim();
		address.replaceAll("；", ",");
		address.replaceAll(" ", ",");
		address.replaceAll("，", ",");
		address.replaceAll(";", ",");
		address.replaceAll("|", ",");
		return address.split(",");
	}
}
