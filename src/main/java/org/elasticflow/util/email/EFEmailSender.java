package org.elasticflow.util.email;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.mail.Address;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.context.support.WebApplicationObjectSupport;

/** 
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */

@Component
public class EFEmailSender extends WebApplicationObjectSupport {
	private static Logger log = LoggerFactory.getLogger(EFEmailSender.class);
	private ExecutorService mailTaskExecutor = Executors.newFixedThreadPool(5);

	public void sendHtmltMailByAsynchronousMode(final String subject,
			final String content) {
		mailTaskExecutor.execute(new Runnable() {
			public void run() {
				try {
					boolean b = sendHtmlMailBySynchronizationMode(subject,
							content);
					if (b) {
						log.info("email send success");
					} else {
						log.error("email send error!");
					}
				} catch (Exception e) {
					log.error("AsynchronousMode send email has a exception:", e);
				}
			}
		});
		mailTaskExecutor.shutdown();
	}

	public boolean sendHtmlMailBySynchronizationMode(String subject,
			String content) {
		EmailConfig emailInfo = getEmailConfig(subject);
		emailInfo.setSubject(subject);
		emailInfo.setContent(content);
		return sendHtmlMail(emailInfo);
	}

	public EmailConfig getEmailConfig(String fromName) {

		EmailConfig emailInfo = getEmailConfig();
		emailInfo.setName(fromName);
		return emailInfo;
	}

	public EmailConfig getEmailConfig() {

		EmailConfig emailInfo = (EmailConfig) super.getApplicationContext()
				.getBean("javaxEmailBean");
		Properties mailConfigBean = (Properties) super.getApplicationContext()
				.getBean("mailConfigBean");

		Properties emailProperties = new Properties();
		emailProperties.put("mail.smtp.host",
				mailConfigBean.getProperty("mail.host"));
		emailProperties.put("mail.smtp.port",
				mailConfigBean.getProperty("mail.port"));
		emailProperties.put("mail.smtp.connectiontimeout",
				mailConfigBean.getProperty("mail.smtp.connectiontimeout"));
		emailProperties.put("mail.smtp.timeout",
				mailConfigBean.getProperty("mail.smtp.timeout"));
		emailProperties.put("mail.smtp.auth",
				mailConfigBean.getProperty("mail.smtps.auth"));
		emailProperties.put("mail.debug", "true");
 
		emailInfo.setProperties(emailProperties);

		return emailInfo;
	}

	private boolean sendHtmlMail(EmailConfig mailInfo) {

		EFAuthenticator authenticator = null;
		Properties pro = mailInfo.getProperties();
		if (mailInfo.isValidate()) {
			authenticator = new EFAuthenticator(mailInfo.getUserName(),
					mailInfo.getPassword());
		}
		Session sendMailSession = Session
				.getDefaultInstance(pro, authenticator);
		try {
			Message mailMessage = new MimeMessage(sendMailSession);
			Address from = new InternetAddress(mailInfo.getFromAddress(),
					mailInfo.getName());
			mailMessage.setFrom(from);
			new InternetAddress();
			InternetAddress[] toListAdd = InternetAddress.parse(mailInfo
					.getToAddress());
			mailMessage.setRecipients(Message.RecipientType.TO, toListAdd);
			if (mailInfo.getCcAddress() != null) {
				new InternetAddress();
				InternetAddress[] ccListAdd = InternetAddress.parse(mailInfo
						.getCcAddress());
				mailMessage.setRecipients(Message.RecipientType.CC, ccListAdd);
			}
			mailMessage.setSubject(mailInfo.getSubject());
			mailMessage.setSentDate(new Date());
			Multipart mainPart = new MimeMultipart();
			BodyPart html = new MimeBodyPart();
			html.setContent(mailInfo.getContent(), "text/html; charset=utf-8");
			mainPart.addBodyPart(html);
			mailMessage.setContent(mainPart);
			Transport.send(mailMessage);
			return true;
		} catch (Exception e) {
			log.error("sendHtmlMail Exception", e);
		}
		return false;
	}
}