package org.elasticflow.notifier.emailer;

import java.io.Serializable;
import java.util.Properties;

/** 
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */

public class EmailConfig implements Serializable {
	private static final long serialVersionUID = -1490893105157071496L;
	private String mailServerHost;
	private String mailServerPort = "25";
	private String fromAddress;
	private String fromName;
	private String toAddress;
	private String ccAddress;
	private String userName;
	private String password;
	private boolean validate = false;
	private String subject;
	private String content;
	private String[] attachFileNames;
	private Properties properties;

	public EmailConfig(String mailServerHost, String fromAddress,
			String fromName, String toAddress, String ccAddress,
			String userName, String password, String subject, String content) {
		super();
		this.mailServerHost = mailServerHost;
		this.fromAddress = fromAddress;
		this.fromName = fromName;
		this.toAddress = toAddress;
		this.ccAddress = ccAddress;
		this.userName = userName;
		this.password = password;
		this.subject = subject;
		this.content = content;

	}

	public String getMailServerHost() {
		return mailServerHost;
	}

	public void setMailServerHost(String mailServerHost) {
		this.mailServerHost = mailServerHost;
	}

	public String getMailServerPort() {
		return mailServerPort;
	}

	public void setMailServerPort(String mailServerPort) {
		this.mailServerPort = mailServerPort;
	}

	public boolean isValidate() {
		return validate;
	}

	public void setValidate(boolean validate) {
		this.validate = validate;
	}

	public String[] getAttachFileNames() {
		return attachFileNames;
	}

	public void setAttachFileNames(String[] fileNames) {
		this.attachFileNames = fileNames;
	}

	public String getFromAddress() {
		return fromAddress;
	}

	public String getName() {
		return fromName;
	}

	public void setFromAddress(String fromAddress) {
		this.fromAddress = fromAddress;
	}

	public void setName(String fromName) {
		this.fromName = fromName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getToAddress() {
		return toAddress;
	}

	public void setToAddress(String toAddress) {
		this.toAddress = toAddress;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String textContent) {
		this.content = textContent;
	}

	public String getCcAddress() {
		return ccAddress;
	}

	public void setCcAddress(String ccAddress) {
		this.ccAddress = ccAddress;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

}
