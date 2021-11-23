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

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */

public class EFEmailSender {
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
		if(GlobalParam.SEND_EMAIL) {
			EmailConfig emailInfo = getEmailConfig(subject);
			emailInfo.setSubject(subject);
			emailInfo.setContent(content);
			return sendHtmlMail(emailInfo);
		}
		return true;
	}

	public EmailConfig getEmailConfig(String fromName) {

		EmailConfig emailInfo = getEmailConfig();
		emailInfo.setName(fromName);
		return emailInfo;
	}

	public EmailConfig getEmailConfig() {		
		Properties p = Common.loadProperties(GlobalParam.CONFIG_PATH+"/mail.properties");
		EmailConfig EC = new EmailConfig(p.getProperty("mail.host"), p.getProperty("mail.From"), 
				p.getProperty("mail.FromName"), p.getProperty("mail.Address"), p.getProperty("mail.Cc"), 
				p.getProperty("mail.username"), p.getProperty("mail.password"), p.getProperty("mail.Subject"), p.getProperty("mail.Content")); 
		return EC;
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