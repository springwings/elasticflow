package org.elasticflow.notifier;

import java.util.Date;
import java.util.Properties;

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
import org.elasticflow.notifier.emailer.EFAuthenticator;
import org.elasticflow.notifier.emailer.EmailConfig;
import org.elasticflow.util.Common;
import org.elasticflow.yarn.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */

public class EFEmailNotifier implements EFNotify {

	private static Logger log = LoggerFactory.getLogger(EFEmailNotifier.class);

	@Override
	public boolean send(final String subject,final String instance, final String content,final String errorType, final boolean sync) {
		if (sync) {
			return sendHtmlMailSyncMode(subject,instance,content,errorType);
		} else {
			sendHtmltMailAsyncMode(subject,instance,content,errorType);
		}
		return true;
	}

	public void sendHtmltMailAsyncMode(final String subject, final String instance,final String content,final String errorType) {
		Resource.threadPools.execute(() -> {
			try {
				boolean b = sendHtmlMailSyncMode(subject,instance, content,errorType);
				if (b) {
					log.info("email send success!");
				} else {
					log.error("email send error!");
				}
			} catch (Exception e) {
				log.error("Asynchronous Mode send email exception:", e);
			}
		});
	}

	public boolean sendHtmlMailSyncMode(final String subject, final String instance,final String content,final String errorType) {
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
		Properties p = Common.loadProperties(GlobalParam.SYS_CONFIG_PATH + "/mail.properties");
		EmailConfig EC = new EmailConfig(p.getProperty("mail.host"), p.getProperty("mail.From"),
				p.getProperty("mail.FromName"), p.getProperty("mail.Address"), p.getProperty("mail.Cc"),
				p.getProperty("mail.username"), p.getProperty("mail.password"), p.getProperty("mail.Subject"),
				p.getProperty("mail.Content"));
		return EC;
	}

	private boolean sendHtmlMail(EmailConfig mailInfo) {

		EFAuthenticator authenticator = null;
		Properties pro = mailInfo.getProperties();
		if (mailInfo.isValidate()) {
			authenticator = new EFAuthenticator(mailInfo.getUserName(), mailInfo.getPassword());
		}
		Session sendMailSession = Session.getDefaultInstance(pro, authenticator);
		try {
			Message mailMessage = new MimeMessage(sendMailSession);
			Address from = new InternetAddress(mailInfo.getFromAddress(), mailInfo.getName());
			mailMessage.setFrom(from);
			new InternetAddress();
			InternetAddress[] toListAdd = InternetAddress.parse(mailInfo.getToAddress());
			mailMessage.setRecipients(Message.RecipientType.TO, toListAdd);
			if (mailInfo.getCcAddress() != null) {
				new InternetAddress();
				InternetAddress[] ccListAdd = InternetAddress.parse(mailInfo.getCcAddress());
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